# Static-collection facts v3 — integration report

Date: 2026-08-02 UTC
Stack base: `origin/agent/static-collections-quality/pr-a-ontology` @
`3164e984d04208fcff5618c49271a4633d304eab`
Implementation branch: `integration/static-collection-facts-v3`
Latest verified code SHA for the targeted boundary rerun:
`eba9b0de6cc45d45a4a19bd9eae2c2b6454a8d85`.

## Executive result

| Gate | Result | Evidence |
|---|---|---|
| Facts-v3 code/contracts | PASS | one existing adjudication call, v3 schema, three independent decisions, strict source quotes, receipts, merge/projection |
| Unit/contract suite | PASS | 124 integrated tests on the final documented worktree |
| Provider-path regression | PASS | audit `status=pass`, `allowlisted_debt=0`, `unapproved=0` |
| Fresh production snapshot | PASS | `quick_check=ok`; 6,990 events; SHA below |
| Read-only plan | PASS | 50/50 exact event/source bindings; zero logical changes |
| Primary-only real replay | **NO-GO** | one Gemma physical send per source and zero writes, but provisional-label recall failed and the run exposed contradictory seed rows |
| Targeted boundary correction | PARTIAL PASS | child-author false confirmations now fail closed; real 7372 joint yoga confirms all three facts; 6562 exposes a seed/ontology contradiction |
| Fallback/failure real drill | NOT RUN | blocked by Gate B ordering; unit call-accounting/fail-closed coverage passes |
| Production-copy apply/warm | NOT RUN | intentionally stopped after Gate B NO-GO |
| Normal Telegram/VK/parser real replay | NOT RUN | intentionally stopped after Gate B NO-GO |
| Fly canary | BLOCKED | exact SHA is not merged to `origin/main`, and earlier gates are red |
| Semantic publication | BLOCKED | no Astro/routes/navigation/sitemap/manifests/public labels changed |

The code increment exists and is testable, but it must not be described as
production-accepted facts v3. The mandatory real-data gate did its job: it
found both model/prompt issues and source-bound provisional rows that contradict
ontology v2.

## Implemented contract

- `STATIC_COLLECTION_FACTS_POLICY_VERSION=static-collection-facts-v3`;
- `STATIC_COLLECTION_ADJUDICATION_SCHEMA_VERSION=static-collection-adjudication-v2`;
- one existing `collection_candidate_adjudication` request returns admission,
  three audience facts and people appearances;
- `gemma-4-31b-it` is primary; per-call Google model fallback, retry, JSON-repair
  expansion and key rotation cannot create a second primary physical send;
- the existing GPT-4o path is the only optional fallback and is separately
  counted;
- trace records physical sends before provider invocation, actual model path,
  rate-limit waits, latency and available token usage;
- every non-unknown audience fact requires an exact continuous quote and is
  revalidated against the persisted same-event `EventSource.source_text`;
- joint confirmed requires independently confirmed child and family facts;
- merge is per-key with manual lock, official/high/medium/low trust, recency and
  same-hash no-op;
- valid all-unknown evaluations receive a bounded per-source/input-hash receipt,
  enabling zero-call/zero-write warm replay;
- legacy `audience_decision` is a deterministic, provenance-bearing projection,
  not a second LLM output;
- audience-only backfill cannot change `is_free` or unrelated fields;
- plan/evaluate use a disposable DB backup; apply is explicit, ID-bounded and
  logical-diff checked;
- report schema is committed as
  `docs/review-data/static_collection_facts_v3_real_data_report.schema.json`.

## Local deterministic verification

Integrated suite:

```bash
TMPDIR=/dev/shm /home/dev/.codex/venvs/events-bot-new/bin/python -m pytest \
  tests/test_smart_event_update.py tests/test_event_update_merge.py \
  tests/test_google_ai_client.py tests/test_static_collection_backfills.py \
  tests/test_static_collection_facts_backfill_report.py \
  tests/test_static_collection_review_seed.py \
  tests/test_static_collection_data_quality_reviews.py -q
# 124 passed in 5.57s
```

Focused core/backfill verification during the real-data refinements:

```bash
TMPDIR=/dev/shm /home/dev/.codex/venvs/events-bot-new/bin/python -m pytest \
  tests/test_smart_event_update.py tests/test_event_update_merge.py \
  tests/test_static_collection_backfills.py \
  tests/test_static_collection_facts_backfill_report.py -q
# 56 passed in 5.53s
```

Provider bypass audit:

```bash
python scripts/inspect/audit_google_ai_provider_paths.py --json
# status=pass; summary.allowlisted_debt=0; summary.unapproved=0
```

## Fresh production evidence

Snapshot command:

```bash
set -a; source /home/dev/.config/fly/release.env; set +a
export PATH="$HOME/.fly/bin:$PATH"
./scripts/sync_prod_db.sh \
  --output artifacts/db/static-collection-facts-v3/prod-20260802T121940Z.sqlite
```

Snapshot:

- `PRAGMA quick_check=ok`;
- event count `6990`;
- SHA-256
  `35ffbaa90af32951de444baeb10dd708f8ef13bfa9b3aca3952d5695bcf1710e`;
- the PR-A union plus named boundaries bound exactly to 50 real source rows;
- all 46 PR-A source refs still matched the fresh production source-text hash;
- frozen 50-row manifest SHA-256
  `3248ab00f40f33a1bd7ce1bde3984c344c34919352e5d8e63a112ba06ae5fcb3`.

No SQLite snapshot or secret is committed.

## Read-only plan

Exact command (paths are repository-local artifacts):

```bash
TMPDIR=/dev/shm python scripts/backfill_static_collection_facts.py \
  --db artifacts/db/static-collection-facts-v3/prod-20260802T121940Z.sqlite \
  --plan --current-date 2024-01-01 --reason audience \
  --event-id-file artifacts/static-collection-facts-v3/primary-event-ids.json \
  --source-id-file artifacts/static-collection-facts-v3/primary-source-ids.json \
  --limit 100 --max-sources-per-event 2 \
  --output artifacts/static-collection-facts-v3/primary-plan.json
```

Result: 50 requested, 50 resolved, 50 exact source bindings, no truncation,
provider calls 0, writes 0, logical before/after hash equal. Report SHA-256:
`2ad8ec3c334fa5790fecaf3a780492675275cbfff593140524bce7923f03ee44`.

## Primary-only real provider replay

The first attempt failed closed before any provider send because local `.env`
pointed at a legacy limiter RPC response without
`google_ai_project_model_atomic_v1`. No unsafe local-limiter override was used.
After checking the canonical gateway documentation and Fly configuration, the
existing approved personalization Supabase URL/secret were aliased to the
required dedicated limiter variables without printing values. Every later send
has a non-empty `quota_scope` and exact limiter contract.

Final 50-source command used `--evaluate --primary-only`, exact event/source
files, `--reason audience`, current date `2024-01-01`, limit 100 and max two
sources per event. Artifact:
`primary-only-final.json`, SHA-256
`4029d4027c2da99fe0dd348a4f16641de4ed22aa6138619f2e0f8c1fbd1c9854`.

Transport/safety results:

- provider logical calls: 50;
- physical sends: 50;
- maximum per routed source: 1;
- requested/actual model: `gemma-4-31b-it` /
  `models/gemma-4-31b-it`;
- GPT-4o fallbacks: 0;
- DB writes: 0;
- logical DB changes: 0;
- valid outputs: 46; deferred: 4 (one recorded provider 500 and three
  fail-closed validation outcomes);
- exact quote rate among 26 non-unknown audience decisions: 26/26 = 100%.

Metrics against the provisional PR-A labels (evaluation SHA-256
`e67d47d26136214c4518276ecd98b39e5223cf63b038ac078a23fdc852bed6f7`):

| Label | Recall | False confirmed vs provisional negatives |
|---|---:|---:|
| child_directed | 12/18 = 0.667 | 2 (`6737`, `6797`) |
| family_suitable | 7/14 = 0.500 | 1 (`6562`) |
| joint_family_activity | 0/4 | 0 |

This is a mandatory NO-GO, not a warning.

## What the real run discovered

### Runtime defects fixed after the full run

1. The first strict validator acted as a second broad keyword classifier and
   rejected valid real wording from 7326. It now leaves meaning to the LLM and
   retains only narrow age-only/child-author/family-atmosphere/tournament
   safeguards. A repeat of 7326 produced child+family confirmed, joint unknown,
   one physical send and zero writes.
2. Audience-only sources with many named people could exhaust the output budget.
   The prompt now makes `candidate_reasons` a real scope: unrequested admission
   and people fields return neutral values. The real 6871 repeat fell from a
   truncated 1000-token response to a valid 231-token all-audience-unknown
   result. The hard limit remains within the documented 900–1100 target.
3. Child-author evidence variants `16 рисунков учащихся` and `выставка
   творческих работ воспитанников` are now narrow fail-closed guards.
4. `Парные упражнения … родитель + ребенок` is now accepted as explicit joint
   activity. The real 7372 rerun confirmed child, family and joint with three
   exact quotes.

Targeted latest-SHA boundary artifact
`boundary-fix-evaluate.json` has SHA-256
`0ff8a0bd9b467bd8c4ea9f97760ee0fc76fa55982edaba157169b46daa195611`:
four Gemma sends, zero writes; 6737/6797 were deferred rather than falsely
applied, 7372 confirmed all three, and 6562 confirmed family.

### Provisional dataset contradictions; not owner gold

The acceptance run found source-bound rows that cannot be used as truth without
another PR-A review. This report does **not** create owner gold or alter scores:

- `6562` is marked a family hard negative, but its raw source literally says
  `Приходите всей семьёй!`; under ontology v2 that is an explicit family
  invitation. The model's family confirmation is not a semantic false positive.
- `7102` is marked positive for family and joint activity, but the raw source
  only says the illustrator appears `с сыном Василием`; it does not invite a
  parent/child audience or promise a joint participant task.
- `7258` says games suit any company/age and suggests gathering with loved ones,
  but never explicitly establishes parent+child attendance or one joint family
  team.
- `7290` contains a title marked `семейный` and a 6+ rating, but no explicit
  adult+child invitation or joint work; those signals are declared insufficient
  by the ontology.
- `7172` and `7176` are essentially title plus age rating; they cannot be
  source-grounded child positives under the v3 rule.
- `6898` has a children’s zone inside a city event, not explicit joint family
  attendance for the whole event.

These are provisional review corrections/needs-source-review, not owner-gold
labels. Forcing the prompt to reproduce them would train the runtime against the
accepted ontology.

## Why later gates were not run

The canonical sequence requires Gate B before fallback drill, copy apply, normal
ingestion and live Fly. Gate B is red, and the task defines any false confirmed
or contradictory real-data evidence as a stop condition. Therefore:

- no production-copy mutation was attempted;
- no warm apply is claimed;
- no Telegram/VK/parser ingestion replay is claimed;
- no production migration/backfill/deploy occurred;
- no Fly code was uploaded or executed from the side branch;
- no route, navigation, sitemap or public publication state changed.

Fly canary is independently blocked by release governance: PRs #207/#222 remain
stack bases and this exact implementation SHA is not reachable from
`origin/main`.

## External consultant evidence

The repository-required Pro-class consultation could not run:

- `a-opus`: Antigravity unavailable in the current location;
- Claude Code project alias `Opus`: not authenticated.

The redacted blocker record is local at
`artifacts/codex/static-collection-facts-v3/external-consultant-blockers.json`.
No lower-class model was presented as an external consultant review.

## Required next step (still not PR B)

1. Correct or move the contradictory provisional rows above to
   `needs_source_review` in PR A; do not create owner gold.
2. Rebase this implementation onto the corrected PR-A head.
3. Re-run the complete 50-source primary-only report on the exact final SHA.
4. Only if recall/false-confirmed gates pass, run the separate fallback drill,
   bounded copy apply/warm and real ingestion adapters.
5. Merge the stack to `main` before any Fly canary. Publication remains blocked
   throughout.

## GitHub

The implementation is intentionally a separate stacked draft PR, not code in
#226:

- draft implementation PR: https://github.com/onedayonemasterpiece/events-bot-new/pull/233;
- stack base: `agent/static-collections-quality/pr-a-ontology` (PR #222);
- GitHub Actions runs for the implementation branch: https://github.com/onedayonemasterpiece/events-bot-new/actions?query=branch%3Aintegration%2Fstatic-collection-facts-v3.
