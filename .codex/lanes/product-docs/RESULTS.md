# Lane product-docs results

## Status

Complete — documentation-only prototype lane.

## Requirement IDs

- R01 — thank-you/prize presentation.
- R02 — manual system/update email templates and checklist only.
- R09 — automatic/operator programme end states and personalization continuity.

## Branch

`agent/focus-group/product-docs`

## Worktree

`/home/dev/.codex/worktrees/events-bot-new/focus-product-docs`

## Base SHA

`6e844eb3`

## Head SHA

`HEAD` (the commit containing this record; exact immutable SHA is reported in
the lane handoff because a Git commit cannot embed its own hash).

## Files changed

- `docs/backlog/features/static-site-focus-group/product-prototype.md`
- `docs/backlog/features/static-site-focus-group/manual-email-templates.md`
- `.codex/lanes/product-docs/RESULTS.md`

## Commands run

- `pwd`
- `git branch --show-current`
- `git status --short`
- `git rev-parse HEAD`
- `sed` on the three canonical focus-group inputs
- `find`/`rg` for related focus-group and personalization references
- Targeted web research against the official Act.Opus site and W3C primary guidance
- Targeted primary-source research against Google PAIR, W3C meter guidance and
  Hu/Koren/Volinsky's implicit-feedback paper
- `python3` documentation contract/local-link verification
- `grep` whitespace verification
- `git diff --check`
- `git diff --cached --check`
- `git status --short`

## Tests/verification

- PASS — all three owned files exist and are non-empty.
- PASS — required R01 phrases cover one pair as two invitations for any partner
  performance, partner logo attribution and no feedback/share/invite multiplier.
- PASS — R02 states manual preparation and one-recipient manual sending only;
  automation/outbox/worker/cron/API/mail merge are out of scope.
- PASS — R09 covers automatic and operator end reasons, synchronous privilege
  expiry, alumni/withdrawn states and personalization continuity.
- PASS — automatic personalized selections are a separate recommendation stream
  with separate opt-in/unsubscribe, interpretable eligibility, prototype-only
  preview and static/manual fallback.
- PASS — preference UI separates tri-state explicit choice, inferred
  `Индекс интереса`, evidence sufficiency and uses `<meter>`, not progressbar.
- PASS — relative Markdown links resolve.
- PASS — only the three lane-owned paths are changed.
- PASS — whitespace/diff checks report no errors.

## Risks

- Partner spelling differs between the requirement (`«Акт-Опус»`) and the
  current official site (`Акт.Опус`); exact spelling/logo usage needs explicit
  partner approval before publication.
- Prize terms, eligibility, legal/privacy review, asset rights and fulfilment
  are intentionally not claimed complete.
- Manual email templates do not prove consent, provisioning, delivery or
  production readiness.
- The recommendation stream is intentionally UI/spec only; there is no sender,
  durable profile or cross-device guarantee in this branch.

## Merge notes

- Documentation-only lane; no code, live sending or production changes.
- Keep all three files together when cherry-picking the lane commit.
- No changes to `site/`, `CHANGELOG.md` or `docs/routes.yml`.
- Product research links are rationale, not evidence of partner prize terms or
  permission to use a logo asset.
