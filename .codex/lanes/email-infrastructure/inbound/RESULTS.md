# Lane inbound Results

## Status
committed

## Requirement IDs
- R04 — Yandex Mail Trigger/Object Storage/YMQ inbound core
- R05 — signed minimized adapter contract and PII-safe logging
- R06 — deterministic fixtures and contract/unit tests
- R07 — plan-by-default isolated Yandex desired-state reconciler/runbook

## Branch
`agent/email-infrastructure/inbound`

## Worktree
`/home/dev/.codex/worktrees/events-bot-new/email-inbound`

## Base SHA
`fa3f24b9bf9514fc7d3db5bd8a1f3cfbfecf1086`

## Head SHA
`f37e52a0` (implementation commit; the following handoff-only commit adds this results file)

## Files changed
- `serverless/email_inbound/` — common canonical contracts, HMAC signing, allowlist logger, intake and delivery functions, runtime README.
- `infra/yandex/email-inbound/` — desired state, conservative reconciler, ZIP build helper and operator runbook.
- `tests/fixtures/email_inbound/` — deterministic Mail Trigger, attachment and YMQ pointer fixtures.
- `tests/test_email_inbound_contract.py`
- `tests/test_email_inbound_intake.py`
- `tests/test_email_inbound_delivery.py`
- `tests/test_email_inbound_reconcile.py`
- `docs/operations/email-delivery.md`
- `docs/architecture/personalization-data-ownership.md`
- `.env.example`
- `CHANGELOG.md`
- `.codex/lanes/email-infrastructure/inbound/RESULTS.md` (handoff metadata only)

## Commands run
- Rebased the clean lane onto `integration/email-infrastructure-release@fa3f24b9` after R03 merged.
- Inspected current Yandex CLI help for folder, IAM, KMS, storage, function, trigger and Lockbox command contracts.
- Created an isolated temporary Python 3.12 virtual environment and installed the repository-pinned `pytest==8.1.1` only.
- Ran targeted tests with `--noconftest` because the repository-wide `tests/conftest.py` imports the full bot runtime and the current host does not have its application dependencies installed.
- Compiled all new Python sources.
- Built both Cloud Function ZIPs into `/tmp/email-inbound-build`, inspected their member lists and imported each extracted `index.handler`.
- Ran scoped secret-pattern and forbidden Fly/core-diff checks.
- Ran `git diff --check` before commit.

## Tests / verification
- PASS: `/tmp/email-inbound-venv/bin/python -m pytest -q -p no:cacheprovider --noconftest tests/test_email_inbound_contract.py tests/test_email_inbound_intake.py tests/test_email_inbound_delivery.py tests/test_email_inbound_reconcile.py` — `25 passed in 0.52s` on the final pre-commit run.
- PASS: `python3 -m py_compile serverless/email_inbound/common/*.py serverless/email_inbound/intake/index.py serverless/email_inbound/delivery/index.py infra/yandex/email-inbound/reconcile.py tests/test_email_inbound_*.py`.
- PASS: `infra/yandex/email-inbound/build-functions.sh /tmp/email-inbound-build`.
- PASS: ZIP inspection asserted `index.py` and `common/contract.py` exist and no `__pycache__`/`.pyc` is shipped.
- PASS: extracted intake and delivery ZIPs both import and expose callable `index.handler`.
- PASS: PII/error-path tests prove provider/transport exception content is neither logged nor chained into platform traceback output.
- PASS: reconciler tests prove all-present idempotency, folder-first staging, explicit secret/queue/trigger operator gates and exact apply confirmation.
- PASS: no staged/diff changes to `main.py`, `main_part2.py`, `db.py`, `scheduling.py`, `fly.toml` or `Dockerfile`.
- PASS: `git diff --check`.
- Not run: full application suite, because this lane is intentionally independent of Fly/core and the host lacks the full bot test dependencies.
- Not run: live Yandex, SpaceWeb or Supabase smoke; the parent explicitly prohibited external mutation in this lane.

## Risks
- Yandex Mail Trigger exposes normalized headers/body and attachment keys, not raw MIME. SpaceWeb must retain the authoritative original; exact raw MIME automation would require an authenticated IMAP puller.
- Cloud Functions trigger messages are limited to 230 KB including metadata. Intake caps the body at 220 KB, but large-message behavior still needs a live canary.
- The installed YC CLI has no YMQ data-plane group. Queue/redrive creation and inventory remain an explicit AWS-compatible operator gate rather than being guessed by the reconciler.
- Yandex documentation currently conflicts on granular `ymq.reader` versus primitive `editor` for a queue trigger. Prove granular access first; any fallback `editor` grant is acceptable only inside the new isolated email folder.
- The Supabase receipt migration and Edge verification endpoint are intentionally omitted because the control-plane lane owns them. Delivery accepts only the documented signed acknowledgement contract.
- No function, queue, trigger, bucket, key, secret, DNS record, mailbox setting or Supabase object was created or mutated.

## Merge notes
- Cherry-pick implementation commit `f37e52a0` and this handoff commit together.
- Keep the control-plane implementation aligned to `kenigevents.email_inbound.adapter.v1` and the exact timestamped HMAC contract in `serverless/email_inbound/README.md`.
- Integrator should rerun the four targeted test files after merging the control-plane lane to catch shared contract drift.
