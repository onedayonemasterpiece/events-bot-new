# INC-2026-08-01 Region Talk final copy used adversative AI cliché

Status: monitoring
Severity: sev2
Service: Region Talk staged editorial writer, operator candidate chat and daily plan
Opened: 2026-08-01
Closed: —
Owners: events-bot / Region Talk
Related incidents: `INC-2026-07-31-region-talk-candidate-chat-incomplete-drafts`, `INC-2026-08-01-region-talk-draft-backfill-nameerror`
Related docs: `docs/features/region-talk-channel/publication-queue.md`, `docs/features/region-talk-channel/llm-verifier-contract.md`

## Summary

The current staged Region Talk writer produced a final Archi.ru candidate with
the adversative construction `не …, а …`. The phrase is grammatical, but in
this product it is a repeated generative-text cliché and violates the expected
editorial voice. Prompt guidance and the existing banned-lexeme validator did
not name or deterministically reject this construction.

## User / Business Impact

- the operator received a publication candidate whose final copy sounded
  templated rather than edited;
- other confirmed candidates could retain the same construction until a new
  writer revision/backfill;
- without a send-time guard, a stale or manually injected draft could enter the
  operator queue or daily plan even after prompt wording changed.

## Detection

The product owner reported the pattern in the Archi.ru candidate. A read-only
production YDB inventory of 140 `publication_candidate_item` rows found the
exact pattern in the Telegram and VK fields of
`https://archi.ru/russia/101203/vsya-mudrost-okeana`.

## Timeline

- 2026-08-01 — product owner reports the final-copy cliché in Archi.ru and asks
  for a candidate backfill plus a permanent pipeline guard.
- 2026-08-01 — read-only YDB audit finds 23 confirmed candidates, 16 currently
  marked `ready_for_operator_review`, and the exact banned construction in both
  Archi.ru platform-copy fields.
- 2026-08-01 — isolated fix branch is created from exact `origin/main`
  `c5e3f6bc79e9`; staged writer/notifier/orchestrator changes and regression
  tests begin.
- 2026-08-01 — PR #184 is merged as `084d2f96c58a` and deployed to Fly release
  v1855 from a clean worktree at exact `origin/main`.
- 2026-08-01 — Archi.ru is regenerated with writer v9/output v3; the new copy
  passes the deterministic guard and is delivered to the operator chat as
  message `33805` with review fingerprint
  `dadfda39ed7600a049d5e3acd6dfe892eba7322f1dab82e0e9d3f187e5cc1e16`.
- 2026-08-01 — all 23 confirmed candidates are selected for bounded backfill
  attempts. The post-write audit finds zero banned-pattern matches; provider
  RPD exhaustion leaves a documented retry/manual-review tail.
- 2026-08-01 20:21 UTC — legacy operator message `33783` is repaired in place
  from the verified current Archi.ru revision; a subsequent candidate-chat
  scan finds zero style-pattern matches in candidate messages.
- 2026-08-01 — the deferred tail is reopened under writer v10. External
  publications also gain a fail-closed publisher reader brief covering outlet
  identity, intended audience and distinctive editorial value from retained
  research evidence before final-copy generation.

## Root Cause

1. Writer and critic prompts discouraged generic language but did not name the
   adversative negation template.
2. Deterministic validation covered clickbait lexemes, length, language,
   voice, and grounding IDs but not sentence-level compositional clichés.
3. Changing prompt prose alone would not invalidate durable staged-call replay,
   terminal backfill status, or the exact operator-review revision.

## Contributing Factors

- prompt-review examples historically used the same contrast construction;
- the previous writer version had no exact-URL force-regeneration control;
- public-readiness validation trusted current-version two-paragraph shape but
  had no independent style detector at render time.

## Automation Contract

### Treat as regression guard when

- changing Region Talk strategy/writer/critic prompts, draft validation,
  backfill selection/versioning, notifier readiness, captions, or daily plans.

### Affected surfaces

- `scripts/region_talk_publication_draft_backfill.py`;
- `scripts/region_talk_goal_notify.py`;
- `scripts/region_talk_orchestrator.py`;
- YDB `publication_candidate_item` draft/review fields;
- operator candidate delivery and future daily plan selection.

### Mandatory checks before closure or deploy

- comma, dash, semicolon, colon and line-break variants fail deterministic
  validation without crossing a sentence/blank paragraph;
- Writer gets one LLM-first retry; the second violation becomes
  `needs_grounding_review`;
- notifier readiness and `public_caption()` fail closed even for a directly
  injected current-version row;
- external-publication readiness fails closed unless all three publisher brief
  dimensions have grounded evidence and the final first paragraph cites them;
- earlier and unversioned writer/backfill fingerprints become actionable,
  while already target-published URL/candidate-ID identities remain immutable;
- focused tests and the full Region Talk suite pass;
- deploy exact clean `origin/main`, run compensating backfill, then verify zero
  banned patterns among unpublished confirmed drafts and record new operator
  delivery fingerprints/message IDs or a truthful zero-delivery result.

### Required evidence

- test output and regex inventory artifact;
- committed SHA reachable from `origin/main` and Fly release/health evidence;
- before/after YDB counts and Archi.ru copy audit;
- post-deploy backfill/catch-up result and operator delivery ledger evidence.

## Immediate Mitigation

The new shared readiness detector blocks any matching draft from operator
delivery and daily planning. Versioned v9 backfill makes all previous staged
drafts stale and actionable without changing the semantic publication verdict.

## Corrective Actions

- add the named Writer/Critic style rule and deterministic detector;
- repeat the guard in draft rendering, notifier readiness, and final caption;
- bump writer/output/backfill/review versions so cached v8 text cannot remain
  current;
- add exact-URL force regeneration and exclude target-published candidates;
- backfill proposed candidates and require fresh reactions for the new exact
  text-plus-media fingerprint.

## Follow-up Actions

- [x] deploy the core guard from exact clean `origin/main`;
- [x] run compensating candidate backfill attempts and deliver the corrected
  Archi.ru operator revision;
- [x] verify zero banned patterns in current unpublished confirmed drafts;
- [ ] finish the quota-deferred retry tail after Gemini RPD reset without
  lowering the model/grounding contract;
- [ ] deploy writer v10 and finish the complete confirmed-candidate catch-up,
  including fresh operator messages and a rebuilt anti-vector plan;
- [ ] observe the next scheduled Region Talk cycle without regression.

## Release And Closure Evidence

- core deployed SHA: `084d2f96c58af6d492ba20ca83a3d0ca03a4bb6b`
  (contains `cb21c118` from PR #184); Fly release v1855, health ready with all
  scheduler checks green.
- follow-up exact-force cooldown SHA: `a2228b70f0eb3b2ede79f9335a8d0a945a9dd1d2`
  from PR #185, merged to `origin/main`; production rollout waits for the
  currently active Region Talk catch-up to finish so deploy cannot interrupt
  another role-scoped session.
- regression checks: focused final-SHA suite `167 passed`; full Region Talk
  suite `705 passed`; independent checklist review passed the final detector,
  selection, readiness, published-identity and orchestration contracts.
- post-deploy YDB verification: 23 confirmed candidates, zero banned matches;
  statuses after the bounded attempts are `ready=1`,
  `media_materialization_pending=4`, `needs_grounding_review=6`,
  `retry_due=12`. The old Archi.ru cliché is replaced by v9 copy and its new
  operator revision is delivered as message `33805`.
- blocker evidence: registered `GOOGLE_API_KEY6`, `GOOGLE_API_KEY5` and
  `GOOGLE_API_KEY2` lanes returned Gemini `RPD`/`RESOURCE_EXHAUSTED`; further
  key guessing stopped because quotas are project/model scoped. No lower model
  or uncontrolled provider path was substituted.

## Prevention

Compositional style policy is now a versioned, testable publication-readiness
contract rather than prompt advice. The regex remains a rejection-only guard;
all semantic rewriting stays in the grounded LLM writer.
