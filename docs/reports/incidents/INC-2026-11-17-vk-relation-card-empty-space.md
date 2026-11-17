# INC-2026-11-17 Empty space in VK relation cards

Status: open
Severity: sev3
Service: VK first-publication relation cards
Opened: 2026-11-17
Owner: KenigEvents (codex)

## Observation and scope

The owner reported post 14712's malformed card. Its one-row relation card
ends at y544 while the footer starts at y1290: 746 px of empty space.
Root inspected the stored public image once; repeated VK reads are excluded.
The same fixed canvas affects new cards with fewer than five rows.
The reported same-programme aliases and conflicting source dates 20/21 are
separate semantic defects; this layout correction does not fix them.

## Cause and correction

vk_event_relations.render_card always allocated 1080x1350 and placed the
footer at 1290. Derive canvas height from the existing row geometry, capped
at the established 1350, and place the footer 60 px above its bottom.
One row becomes 1080x640; two become 1080x878; five stay 1080x1350.
No new modes, assets, state, queues, retries or external calls.

## Regression and acceptance

Run tests/test_vk_event_relations.py, including original-photo ordering,
source avatars, medallions, descriptions and delayed frozen comments.
Inspect before/after one/two/five-row PNGs and compare the five-row pixels.
Check schedule cards both with and without the next-card caption.
Retained evidence: /home/dev/artifacts/events-bot-new/20261112T111614Z-similar-events-check/compact-card-*-20261117.png.
Root inspected one/two-row previews; content-region and five-row pixel
comparisons pass. Initial relation suite 33 PASS (13.39 s); regression controls on the old renderer fail for
one/two rows and pass for five.
Release and acceptance on an ordinary NEW carousel remain open.
Old VK posts and comments are not edited, deleted or recreated.
