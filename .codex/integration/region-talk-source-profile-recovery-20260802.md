# Region Talk source-profile recovery — integration report

Date: 2026-08-02
Base: `origin/main@ba8ab078ba9894ccd5810045b1b8787ecb29d743`
Branch: `integration/region-talk-source-profile-recovery-20260802`
Integration head before PR: `a6da5953e70237b2f60f73d3de0cfc916dce2d71`

## Requirement matrix

| ID | Status | Integrated result |
|---|---|---|
| R01 / P0 regression contracts | Done (local) | Failing-first lane evidence plus focused capture/import/profile/Writer/correction tests; full suite passes. |
| R02 / social capture | Done (code) | Role-scoped Telegram/VK description+pinned+30–80 scan, authored/repost/service/ad classification, 8–16 representative excerpts, stable capture/fingerprint and zero profile-call change decision. |
| R03 / readiness and order | Done (code) | Capture/profile precedes Writer; one current social post is insufficient; missing profile projects an explicit capture request; profile and onboarding-writer stage caps are separate; accepted verdict remains monotonic. |
| R04 / guarded publisher import | Done (code + local validation) | Exact-byte/schema/hash import, serializable atomic rereads/upserts, replay/conflict safety, separate profile/correction/batch/receipt rows and exact-main OIDC workflow. |
| R05 / future publisher evidence merge | Done (code) | Normal external intake merges publisher evidence monotonically by stable profile identity; weaker article evidence cannot replace a richer dossier; locality/scope conflict fails closed. |
| R06 / Writer vNext | Done (code) | Writer v11: 45–110 content hook, grounded source sentence, material details, URL/metatext/prestige/incomplete/cliché guards, deterministic source-aware CTA and rendered-copy revalidation. |
| R07 / supplied profiles and RG correction | Done (local validation) | All three exact sidecars validate; Archi/Peasant profiles are reusable; RG is mixed/needs-review and its exact `reg-szfo` correction remains fail closed. Explicit serializable correction review writes no candidate mutation. |
| R08 / live backfill and operator delivery | Pending release phase | Must run only after PR merge/deploy: protected import/readback, social captures, explicit correction decisions, unpublished regeneration, new operator revisions, fresh reactions and plan rebuild. |
| R09 / acceptance evidence | Partial | Local suite/schema/replay/conflict/zero-provider/workflow checks pass, including unchanged non-ready profile attempts. The 20-message copy audit and zero-autopublish proof depend on R08 live output. |

## Integrated commits

- `3cb63167` — bounded social capture;
- `d431d6e1`, `5f363370`, `a6da5953` — profile lifecycle, late correction fence, capture request projection, explicit correction review and docs;
- `6973194e` — publisher profile/import/workflow and monotonic merge;
- `22866121` — Writer vNext/backfill/notifier/renderer;
- lane evidence commits: `da014375`, `ec22d3aa`, `d499ad19`, `485d4827`.

## Local validation

- `python -m py_compile` for all changed Region Talk runtime scripts: pass.
- `git diff --check`: pass.
- `pytest -q tests/test_region_talk*.py`: **811 passed in 31.57s**.
- focused finalizer/orchestrator after integration version/capture request:
  **163 passed**.
- focused publisher correction/import/finalizer/backfill/orchestrator:
  **214 passed**.
- all three sidecars: Draft 2020-12 schema + semantic dry validation pass,
  one profile/one correction each, `publication_effect=none`:
  - Archi.ru: `f8440fd7d6430386624936c3181bac11936e64da0d26f7641b7c763f3c906666`;
  - Peasant Studies: `0d61c1eac7799e70e677a23eb61537bf8c725aebbd1e8fd035548fde28e37433`;
  - RG: `2bae5d314ec2388b6a5033ef233e04dce4cf29e471e61237585157ff05918f1e`.
- guarded workflow YAML parse and every embedded `run` block `bash -n`: pass.
- `scripts/inspect/audit_google_ai_provider_paths.py`: pass,
  `unapproved=0`, `allowlisted_debt=0`.

Local receipts are ignored operational artifacts under
`artifacts/codex/region-talk-source-profile-recovery/integration/`.

## Safety / production effect before merge

- No publisher profile was written to live YDB from the integration branch.
- No Telegram/VK role session was opened for live source capture.
- No operator revision or target-channel publication was sent.
- No candidate verdict/manual-review/publication permission was promoted.
- Production publication effect: **none**.

## External consultant evidence

The repository-approved Pro/Opus review paths were attempted and unavailable:

- `a-opus`: Antigravity unavailable in the current location;
- Claude Code project alias `Opus`: no active login;
- Gemini 3.1 Pro through `a-gemini`: same Antigravity availability blocker.

The exact command/provider evidence is retained as ignored artifacts under
`artifacts/codex/region-talk-source-profile-*20260802.txt`. No Flash/Lite probe
is represented as an external consultant review.

## Release closure still required

1. push branch, open separate PR, wait for CI/review and merge exact head to
   `main`;
2. deploy a clean exact `origin/main` checkout;
3. run protected publisher imports and strong exact YDB readback;
4. review all queued corrections, with RG recorded as `block_regional` unless
   genuinely fresh evidence reverses locality;
5. build bounded profiles for current unpublished social candidates without
   concurrent use of the role-scoped Telegram session;
6. regenerate/deliver only current unpublished candidates, require fresh
   reactions, rebuild the anti-vector plan and audit 20 current revisions;
7. record message IDs, receipts and before/after proof that no target-channel
   publication occurred.
