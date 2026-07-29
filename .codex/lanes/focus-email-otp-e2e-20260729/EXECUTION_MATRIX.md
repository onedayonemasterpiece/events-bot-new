# Focus email OTP E2E execution matrix

| ID | Requirement | Area | Likely files | Dependencies | Conflict risk | Lane | Parallelizable? | Done when |
|---|---|---|---|---|---|---|---|---|
| R01 | Receive a real auth email without opening a mailbox and complete E2E | Yandex Cloud / Supabase Auth | `artifacts/`, Yandex Mail Trigger resources | YC OAuth, live candidate, private ingress | High | integrator | No | Mail Trigger receives the message and the browser reaches a real signed-in session |
| R02 | Put the OTP code in the email subject | Supabase Auth template | hosted Auth config, canonical docs | Current SMTP/template capability | High | integrator | No | Subject template contains the same six-digit token as the message |
| R03 | Provide a polished, simple HTML/text OTP email | Product/email UX | hosted Auth config, docs | R06 research | Medium | integrator | After receipt, subject/body/link/code are verified from the real message |
| R04 | Keep both confirmation by link and by code | Auth UX | focus invitation page/component | Supabase magic-link/OTP contract | High | integrator | Both one-time alternatives create a valid session in isolated runs |
| R05 | Numeric mobile keyboard and automatic submission after the last digit | Mobile auth UI | focus invitation component, tests | OTP length | Medium | integrator | `inputmode=numeric`, one-time-code autocomplete, six digits, no Enter/button required |
| R06 | Check Pinterest and accepted OTP UX patterns | Product research | Pinterest idea library, UI review notes | None | Low | research + integrator | Yes, read-only | References are reviewed and the implementation documents adapted patterns |
| R07 | Preserve 30-day focus membership independently of auth/personalization | Focus-group lifecycle | existing membership runtime, tests | R04/R05 | High | integrator | Auth, logout and personalization reset do not delete membership |
| R08 | Clean up temporary users, secrets and test artifacts safely | Operations/security | Supabase admin, Yandex private storage | R01–R07 | High | integrator | Disposable identity is removed and no secret is committed or printed |

## Closure 2026-07-29

| ID | Status | Evidence |
|---|---|---|
| R01 | Done | Real Yandex Cloud Mail Trigger delivery; browser session created |
| R02 | Done | Same six-digit code verified in hosted subject and body |
| R03 | Done | Final live body has brand, 10-minute copy, ignore/security copy and no external images |
| R04 | Done | Independent numeric and PKCE-link issuances succeeded; both replays rejected |
| R05 | Done | Five digits made zero verify calls; digit six made exactly one without Enter |
| R06 | Done | 60 references reviewed across 10 query families; 10 patterns shortlisted |
| R07 | Done | Existing focus membership-independence contracts remain green |
| R08 | Done | Global refresh-session logout, disposable Auth user deletion and temporary-secret cleanup |

Sanitized runtime receipts remain under the ignored
`artifacts/codex/focus-email-live-e2e-20260729/` directory. No receiver address,
OTP, confirmation URL, SMTP password, session token or service key is retained
in Git.
