# SpaceWeb outbound mailbox canary

## Outcome

**Partial / controlled submission proven.** Exactly one message was submitted from
the newly created SpaceWeb mailbox through authenticated Roundcube webmail. The
message exists exactly once in the SpaceWeb `Sent` folder and the sender mailbox
has no delivery-failure/bounce item. Recipient-side arrival, placement and
`Authentication-Results` cannot be asserted because this environment has no
approved credentials/session for the `info@kgd80.ru` Yandex mailbox.

No second message was sent after the success notification proved too transient
for the browser assertion. The `Sent` copy, raw source and lack of a bounce were
used instead.

## Canary evidence

| Field | Evidence |
|---|---|
| From | `info@kenigevents.ru` |
| To | `info@kgd80.ru` (one recipient, no Cc/Bcc) |
| Subject | `[KenigEvents mail canary] KE-SPACEWEB-OUTBOUND-20260712T065500Z-51db6e` |
| Correlation | `KE-SPACEWEB-OUTBOUND-20260712T065500Z-51db6e` |
| Message-ID | `<6756a6ea3665697088b0e9cd25ce3bb3@kenigevents.ru>` |
| Date | `Sun, 12 Jul 2026 09:55:38 +0300` |
| Sender folder | One item in SpaceWeb `Sent`, timestamp `09:55` |
| Sender failure scan | No correlation or delivery-failure terms in SpaceWeb `INBOX` or `Junk` as of `2026-07-12T06:59:53Z` |
| Reply-To | No explicit `Reply-To` header in the raw Sent copy; normal replies therefore target the `From` address |

The raw SpaceWeb Sent copy contains no `DKIM-Signature` or
`Authentication-Results`. This is expected evidence only of the submitted copy,
not proof of the final MTA-added signature or the receiving Yandex MX decision.
Those headers must be taken from the recipient's raw message. Do not describe
this lane as recipient-delivery or spam-placement acceptance until that mailbox
is inspected.

## Transport decision and diagnostics

SpaceWeb's current official documentation specifies authenticated implicit TLS
SMTP at `smtp.spaceweb.ru:465`, with the full mailbox address as the username:

- <https://help.sweb.ru/nastrojka-smtp-na-sajte_973.html>
- <https://help.sweb.ru/entry/1240/>

After the two earlier similar port-465 timeouts, the external-tool research gate
was observed before one final instrumented transport probe:

```bash
getent ahostsv4 smtp.spaceweb.ru
timeout 15 openssl s_client \
  -connect smtp.spaceweb.ru:465 \
  -servername smtp.spaceweb.ru -brief </dev/null
```

DNS resolved to `77.222.41.136`, but the TLS connection timed out with exit code
`124`. No further SMTP guesses, alternate ports or plaintext transport were
attempted. The allowed authenticated SpaceWeb webmail fallback was used.

Webmail procedure, with secret values excluded:

1. Open the authenticated SpaceWeb control panel and the
   `info@kenigevents.ru` mailbox.
2. Read the existing mailbox password from the temporary mode-`0600` file
   populated from deletion-protected Lockbox.
3. Fill the password, immediately unlink the file, and enter Roundcube.
4. Prepare one recipient, the unique subject/body marker, and verify exactly one
   visible Send control.
5. Click that control once.
6. Re-enter read-only, verify one matching `Sent` item, open its raw source and
   scan sender `INBOX`/`Junk` for failure notices.

Every temporary `/tmp/ke-spaceweb-info-password` instance was deleted
immediately after credential fill; final `test ! -e` passed. No password, cookie,
raw body or private key was written to Git or this report.

## Public authentication prerequisites

Read-only DNS checks after submission passed:

```bash
dig +short MX kenigevents.ru
dig +short TXT kenigevents.ru
dig +short TXT sweb._domainkey.kenigevents.ru
dig +short TXT _dmarc.kenigevents.ru
```

- MX: `10 mx1.spaceweb.ru.`, `20 mx2.spaceweb.ru.`
- exactly one root SPF record is present;
- `sweb._domainkey` publishes a `v=DKIM1` record;
- DMARC remains monitoring-only: `p=none`, relaxed DKIM/SPF alignment.

These DNS checks do not replace recipient-side SPF/DKIM/DMARC results.

## Remaining acceptance action

Open the single message in `info@kgd80.ru` and record, from its raw headers:

1. delivery folder (`Inbox` or `Spam`);
2. every `Authentication-Results` header;
3. every `DKIM-Signature` domain (`d=`) and selector (`s=`);
4. SPF, DKIM and DMARC pass/fail plus alignment;
5. exact `From`, `Reply-To` behavior and Message-ID match.

This is a read-only owner-side verification; it does **not** require another
canary.

## Provenance

- Worktree: `/home/dev/.codex/worktrees/events-bot-new/email-followup-smtp`
- Branch: `agent/email-release-followup/smtp-canary`
- Base: `origin/main@d09948130e26bea9f2294248f0b987940bc5b869`
- Pre-commit head: `d09948130e26bea9f2294248f0b987940bc5b869`
- Scope: this results file only; no provider, DNS, mailbox-purpose, tariff, code
  or shared-document mutation.
