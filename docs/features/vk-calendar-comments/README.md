# Calendar files in Afisha comments

Ordinary one-shot calendar delivery was restored on November 13 after the
owner-requested live pilot. The earlier November 12 containment is historical.
A successful `wall.get` still does not prove document writes are available.

Only a newly created postponed event announcement schedules a calendar comment.
The successful `post_to_vk` call knows its exact publication timestamp and adds
one `vk_calendar_comment` task to the existing `JobOutbox`, with `next_run_at`
equal to publication time plus a stable, nonuniform 5–15 minute delay.
This delay belongs to the application; VK has no native delayed comment.

At that time the existing worker resolves this specific announcement's public
URL with one service-token wall read, persists the live URL, builds the existing
ICS calendar and uploads/attaches it in a community comment. An idle worker
never polls our wall. External source imports remain independent.

There is no historical catch-up, recurring comment reconciliation, template or
filename re-upload, or automatic editing/deleting of delivered comments.
The existing receipt retains the uploaded document and delivered comment ID.
Only recovery of a lost comment response reads comments, using that saved
exact document; normal delivery does not read comments or re-read the post.
Existing outbox retries and shared CAPTCHA handling apply without token
rotation or challenge bypass. A new token alone does not prove recovery.

On November 12 the former automatic scan was disconnected in PR 916 after
real `docs.save` code 14 also occurred on the replacement primary token.
The scheduled task replaces that scan; previously published posts lacking a
comment do not create work. Live acceptance requires a normal new post,
its public URL and actual delayed comment, not just passing tests.

## Due task ordering, 14 November

Ordinary user-authored ICS delivery is active in the current runtime. The earlier
containment description above records the November 12 incident, not its current
producer state. The existing JobOutbox priority now gives already due comment
jobs precedence over media-blocking review, old missing-media VK retries and
static builds. The related-only foreground override no longer collapses these
comment jobs into the static/VK FIFO lane. This applies to both ICS and relation
comments, which share the existing task enum. Future tasks remain excluded by
the existing due-time SQL predicate; random delays, pacing, CAPTCHA handling
and the one-job worker are unchanged. An already running operation is not
preempted, so this ordering correction is not a hard wall-clock deadline proof.

## Per-job CAPTCHA limit, 16 November

Repeated `docs.save` code 14 on one new calendar job could pause every VK job
again after each harmless-read recovery. The shared worker now counts only code
14 outcomes in the existing job payload (`captcha_attempts`) and applies the
existing `CAPTCHA_MAX_ATTEMPTS` setting, default 2. Ordinary network failures
remain in the separate existing `attempts` counter.

On exhaustion the job ends with `status=error`, `terminal_reason=failed` and a
structured `VK_CAPTCHA_TERMINAL` warning. This means the document/comment was
not delivered. The real challenge still invokes the existing shared cooldown;
marker-scoped recovery resumes other tasks and excludes this failed job. No
write probe, token rotation, challenge bypass or new worker is introduced.

Restart and same-job enqueue retain the CAPTCHA count. Omitting an enqueue
payload retains the frozen post URL, selection and publication time. Ordinary
enqueue cannot clear an exhausted CAPTCHA terminal or trigger another attempt.
New jobs without CAPTCHA and non-VK tasks do not acquire this metadata.
Existing delivered receipts are not changed, replayed or rewritten. Source
checks and mocked tests do not establish that VK has cleared an account block;
ordinary new publication and comment receipts remain the live acceptance.

## CAPTCHA recovery ordering, 17 November

Within one resumed CAPTCHA cohort, frozen text-related comments and new posts
receive paced due slots before calendar document uploads. Already due documents
that retain the CAPTCHA marker also follow ready text comments and VK posts
when a delayed worker picks work. Ordinary documents without that marker retain
the existing foreground comment priority. Marker scope, 30-second recovery pacing
and CAPTCHA budgets are unchanged. Documents remain queued, but recovery backlog
can delay them beyond their original publication window. Neither this order nor
a harmless read proves that VK accepts a write or clears the provider challenge.
