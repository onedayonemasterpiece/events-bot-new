# Smart Update Ultra — DevCoveer component pilot, 2026-10-05

Status: **PARTIAL EMPIRICAL CHECKPOINT — NOT production acceptance**.
Checkpoint: 08:04 UTC. This report is updated at completed milestones; failures and prior prompt outputs are retained. Existing design: [Candidate A](../../design/smart-update-ultra-to-be-2026-10-05.md). PR: #723.

## Scope actually executed

- Reused isolated workspace `handoff:src_5dd70fb837000a90095bdc3d`, base `627b24e2fe776a52f10448b2f955977016deef7a`. The unrelated dirty application checkout was not reset or used for deployment.
- Captured 10 public Telegram messages from four channels using the existing local read-only Telethon launcher: `agropark39` (2342–2344), `ambermuseum` (5624–5626), `tanja_from_koenigsberg` (4164–4165), `progulki_s_katey` (812–813). Nine original JPEG images retained with hashes. Capture observed at 07:42 and 07:48 UTC; publication dates are older and are not presented as today's announcements.
- Ran a direct structured-request transport trial and native OpenCode model tasks. OpenCode reported version 1.18.31 at preflight. Exact provider routes are recorded below.
- Two historical VK-derived text cases and one synthetic negative control were supplied to two text models. This is **not live VK capture**, and the text supplied to these pilot tasks was condensed/emoji-normalized rather than a byte-exact original. The exact delivered prompt must remain part of the receipt.
- No production snapshot was obtained. No real domain CREATE/MERGE on a shadow snapshot has run. No production event, cursor, publication or service change; no Kaggle launch.

Private retained evidence root:
`/home/dev/artifacts/events-bot-new/20261005T073316Z-ultra-public-component-20261005/`

Subdirectories: `cases/` original source packets, `media/` captured images, `runs/` structured transport receipts, `native/` native task exports when collected. Original data is not committed to GitHub. This is a development corpus, **not yet an independently adjudicated golden/holdout corpus**.

## Completed observations

| Trial | Model / task | Observed result | Acceptance boundary |
| --- | --- | --- | --- |
| Transport T0 | `opencode/mimo-v2.6-flash-free`, direct local OpenCode session with image part and JSON schema | HTTP 403 `FreeTierError`: "OpenCode's free tier can only be used from within OpenCode"; application elapsed 3.264 s; zero output | Transport failure, not a semantic model failure; no header/client-identity spoofing or paid fallback attempted. Cause of rejection is not established |
| V1 museum | MiMo, `dvt_3a48ccdd43ad45489d4da555277a3209` | Native task read the captured text/photo and returned a final result; left unknown dates/times null. However, invented a disposition outside the canonical enum and placed the seasonal pause inside `events` | Useful negative finding; not ready for automatic import. Claim that photo has no text requires independent image review |
| V2 museum | MiMo, `dvt_e17a7bcc65684daa883efa0a52025066` | Targeted DEV prompt produced `MIXED`, one undated activity, pause in `UPDATE_DETAILS`, no invented session dates | Semantic defect improved on the same DEV case. Still schema defect: lifecycle `support` is an array although requested as a string; support excerpts contain ellipses. Not holdout success, not full contract acceptance |
| Text V1 | `opencode/nemotron-3-ultra-free`, `dvt_fe20faaf91834e74a6566716529e59de` | Preserved main party date/time/price/age/program; rejected synthetic recap; identified time-change action for Rudau | Rudau disposition is `EVENTS_FOUND` despite nonempty actions; action kind `reschedule` is not the canonical `RESCHEDULE_TIME`. No DB lookup/materialization tested |
| Text V1 | `opencode/big-pickle`, `dvt_e9dc6ea21c1447e397f94152a02ccd3b` | Preserved main party fields/program; rejected recap; Rudau disposition `MIXED` | Action kind `time_changed` is not canonical. This loose text pilot did not supply the full production schema, so it cannot establish native schema compliance |
| Partial-album stress | MiMo, `dvt_dcf2ee0d20bb4061a404548cbb2f20ad` | Still running at checkpoint; resume the existing task, do not create a replacement | No final result counted |

Original text fixtures: [Rudau time change](../../../tests/replays/INC-2026-05-07-vk-time-reschedule-wrong-match/sources.json) and [party / negative control](../../../tests/replays/INC-2026-07-17-vk-auto-provider-quota-false-reject/sources.json).

## Concrete lessons already supported by runs

1. **Joint vision decision + OCR is the desired first-pass contract.** After seeing an image, the same model response must return its readable text and event/lifecycle decision; a second mandatory vision/OCR call is unnecessary. Empty OCR on a genuinely text-free photograph is valid and differs from unreadable or unopened media.
2. Taking the last N Telegram messages can capture only an album tail: all four sampled guide messages have empty captions and `album_needs_assembly`. Corpus acquisition must assemble the complete album and retain source boundaries before counting a negative verdict or lab sample as complete. A video/document-only post is separately marked `document_not_captured`.
3. Native task execution and semantic/schema acceptance are different. Completed tasks above contain material defects. No output is inserted merely because OpenCode says completed.
4. Permission waiting can look like provider slowness: museum task paused on exact `external_directory` reads. One-shot approval of the two already-authorized public inputs resumed it. Future lab/package layout should predeclare these bounded inputs; never solve this with a global allow-all policy. Wall-clock task duration including such waits is not model inference latency.
5. The direct structured transport trial failed, while native OpenCode reading subsequently produced results. This establishes different observed behavior, not the reason and not a general ban on all OpenCode SDK use.
6. Prompt v2 removed one concrete V1 semantic defect but still failed a field-type requirement. Preserve V1, V2, exact inputs, outputs and independent review. Prompt refinement must remain separate from an untouched grouped holdout.
7. Current DevCoveer free-space probes during preparation fell from about 3.4 GB to 1.8 GB while other work existed. This is not attributed to Ultra. Before a DB snapshot, measure actual size and headroom; no full environment/image copy or broad cleanup was performed.

## Golden corpus and database gates still open

Independent labels must come from original texts/images, not tested model outputs or production rows. Group reposts/occurrences across splits. Freeze evaluation time explicitly: these initial tasks used source publication dates, so they measure as-of extraction, not current publication eligibility. The museum case has already been used for tuning and cannot later be called untouched holdout.

Before CREATE/MERGE research: obtain a consistent authorized production snapshot, retain immutable original and provenance, isolate disposable shadow DBs and all publication effects, verify the existing Smart Update entry point and internal writes. Never substitute toy SQL for domain acceptance. A current snapshot containing the sampled posts introduces look-ahead; record it and use explicitly declared counterfactual variants only in shadow copies.

## Excursions: reusable execution, separate domain

The existing guide track explicitly uses `guide_*` tables, separate profiles/templates/occurrences/facts and separate digest eligibility, not ordinary `event`/`daily` surfaces. [Current guide contract](../../features/guide-excursions-monitoring/README.md).

Ultra may share capture, joint vision/OCR, resource routing, durable stages and execution attribution, but needs a guide-specific semantic/result adapter: `scheduled_public` versus `on_request_private`, template/profile-only updates, sold-out/waitlist/cancel/reschedule, separate occurrence identity. An absence of a dated public departure is not equivalent to absence of useful guide/template facts. Existing guide policy and event policy must not be silently conflated.

## Resume checkpoint

Keep the existing handoff and task IDs. First inspect/export terminal receipts; inspect the pending album task and authorize only its exact previously requested public files when appropriate. Save the next milestone here before launching more trials. Next independent work: complete source albums, publish a validated role/schema contract with safety tests, establish an independently reviewed subset, obtain live VK reads and a consistent snapshot through supported access. Blocks on one layer do not make another layer complete.
