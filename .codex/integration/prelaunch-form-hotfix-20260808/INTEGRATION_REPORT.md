# Prelaunch registration hotfix — integration report

Date: 2026-08-08 UTC  
Initial production source: `9d8fc9203a69f385407a57e23310bb47f2db4e2d`  
Released production source: `5a9d804438377f65fe4b26bd7019e73626529864`

## Requirement closure

| ID | Status | Evidence |
|---|---|---|
| R1 | Done | Clean production reproduction reached the real RPC and returned HTTP 400 / SQLSTATE `22023` / `invalid_prelaunch_consent`; one console error corresponded to that response. Protected function inspection proved the browser's v2 consent and live v1 RPC contract had drifted. |
| R2 | Done | Server-confirmed first success hides the form and shows exact «Готово, вы записаны»; reload and repeat show «Вы уже записаны»; only «Другой e-mail» resets. Error paths retain input and the in-flight runtime lock complements the disabled CTA. Unit/contract and artifact/live browser gates pass. |
| R3 | Done | Production has one `UNIQUE(email)` constraint. Migrations v1/v2/v3 are recorded; normalization remains `lower(btrim(...))`; v3 uses `ON CONFLICT(email) DO NOTHING`, updates the one row on repeat and returns `already_registered`. RLS/private table grants remain closed. |
| R4 | Done | Protected operator reads for `source='prelaunch_home'` after `2026-08-08T05:29:24Z` returned zero rows using both first- and last-request timestamps. No public read endpoint was added and no customer email was emitted. |
| R5 | Done | Real production first/repeat RPC results were HTTP 200 `registered` then HTTP 200 `already_registered`; reload state passed. Masked DB proof for `s***@e***.test`: one row, `request_count=2`, timestamps `15:09:56.023762Z` / `15:09:57.035180Z`, consent v1-updates, delivery pending. Cleanup deleted one row; remaining count 0. |
| R6 | Done | PR #373 and rollback-safety PR #375 merged green. Exact-main release [run 31263560430](https://github.com/onedayonemasterpiece/events-bot-new/actions/runs/31263560430) published release `prelaunch-main-31263560430-1`; pointer and ancestry checks match the released SHA. Stable production success/reload/repeat acceptance passed with zero console errors. |

## Lane integration

| Lane | Status | Integration evidence |
|---|---|---|
| L1 production reproduction | completed | real Network/console and protected RPC definition |
| L2 protected DB audit | completed | catalog, migration, customer-window and synthetic-row reads |
| L3 release map | completed | side-branch drift identified; main-based transplant selected |
| L4 incident map | completed | new incident regression contract |
| L5 implementation | completed | merged in PR #373; targeted tests and exact visual-source diff passed |
| L6 integration/release | completed | reviewer rollback finding fixed in PR #375; exact-main release and live acceptance complete |

No worker change was abandoned or left uncommitted. The dirty root checkout was
not used for integration or release.

## Release and live acceptance

- GitHub Actions release: `31263560430`, successful.
- Public pointer: `repo_sha=5a9d804438377f65fe4b26bd7019e73626529864`,
  `source_branch=main`, both form gates `ok`.
- Production browser: first title `Готово, вы записаны`, reload title
  `Вы уже записаны`, explicit reset returns idle form, repeat title
  `Вы уже записаны`; two RPC requests total; no console errors.
- Production DB after two calls: one normalized row and request count two; after
  cleanup: zero synthetic rows.
- Live CSS hash and desktop/mobile/brand image hashes equal the recorded
  pre-release baseline. Title, canonical, description and robots are unchanged.

## Corrective contract

The browser treats localStorage only as a UX hint. Durable result semantics and
deduplication are server truth: `registered` is a new normalized row,
`already_registered` is a successful repeat, and the unique constraint plus
conflict-safe insert prevents a second row. The exact-main publisher now runs
the form state matrix on the built artifact and stable URL and preserves object
metadata during rollback.
