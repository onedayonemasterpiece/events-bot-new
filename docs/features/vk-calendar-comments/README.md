# Calendar files in Afisha comments

Automatic calendar task creation is temporarily disconnected after a single
scheduled `docs.save` still returned code 14 at 20:40:07 UTC on November 12.
The affected task is contained with its receipt preserved; a successful
`wall.get` must not be mistaken for proof that document writes work.
Photo publication remains enabled. The scheduled helper below is retained
for transport repair and tests; it is not currently called by `post_to_vk`.

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
