# Event-page action onboarding and unified calendar follow

Status: **product/design contract; onboarding lab plus opt-in real-event preview implemented; default unsolicited display is off; unified calendar follow not yet production-complete** (2026-07-11).

Canonical UI surface: `/lab/event-decision-block/`, shortlisted layouts V3-A and V3-D.

## Why this exists

V3-D intentionally removes visible words from calendar/share/like controls and leaves only an SVG icon plus a truthful numeric value. This makes the decision sheet calmer, but a bare calendar icon plus `+24` does not explain the full product value. Feature education must be contextual, personal and one-at-a-time rather than a generic first-launch tour.

## Current implementation versus target contract

### Current state in `feature/event-page-ux-lab-v3-20260710`

- `CalendarLink.astro` is a normal static link to the generated event `.ics` file.
- Like/share state on the static-site preview is local-browser personalization state; first-party persistence/live counter hydration remains a separate rollout.
- The action onboarding shown in `/lab/event-decision-block/` remains the design comparison. A real-event noindex trial can be compiled with `PUBLIC_EVENT_PAGE_DECISION_VARIANT=ticket-cluster`; the default event-page build stays unchanged until acceptance.
- The real-event trial stores anonymous suppression/use state in `ke_event_action_onboarding_v1` and one-hint-per-session state in `sessionStorage`. It does not write onboarding state to Supabase and therefore is browser-local rather than authenticated cross-device state.
- Email-follow foundation exists in parallel unfinished work, and its worker is documented as dry-run. The current production event page must not promise a sent email until that integration is merged, deployed and verified.

### Target unified calendar action

User-facing **«В календарь» / calendar icon** is one product action with four coordinated outcomes:

1. open or download the event `.ics` file for the device calendar;
2. save the event to the user's private **«Мои события» / избранное** collection on KenigEvents;
3. for an authenticated user with explicit email consent, create an event follow so reminders and source-grounded date/time/place/cancellation changes can arrive by email;
4. make all followed/saved events available later in one personal place.

The action is successful even when only `.ics` can be opened. The UI must report outcomes honestly:

- `.ics` only: `Файл календаря открыт. Войдите, чтобы сохранить событие здесь и получать письма.`
- authenticated save, email not consented: `Сохранено в «Моих событиях». Письма не включены.`
- authenticated save + email-follow queued: `Сохранено: событие в «Моих», календарь открыт, письма включены.`

Do not say that email was sent when only an outbox/dry-run record exists.

## “Saved” is not the same as “Liked”

- Calendar follow creates a **private saved/follow state** and must not silently inflate the public like counter.
- Heart creates an explicit **«Нравится»** reaction, recommendation signal and public aggregate contribution.
- User-facing wording may call the private collection «Избранное» or «Мои события», but implementation state should remain distinct (`followed/saved` versus `liked`).

## Onboarding policy

### One hint only

At most one action hint may be visible per page and per session. Priority:

1. calendar/follow when the user has never completed it;
2. like when calendar is already understood but the user has never liked an event;
3. share when calendar and like are understood but the user has never shared;
4. no hint when all actions are used, dismissed or already taught in the session.

A hint becomes eligible only when the action row is substantially visible and stable for about `800–1200ms`. Do not show it while gallery/auth/report/modal UI is open.

### Personal state

Anonymous/browser fallback belongs to the consented static-site personalization profile, keyed by its `anon_id`. Authenticated durable state belongs to the personalization Supabase/Postgres project, not Fly SQLite.

Conceptual state:

```json
{
  "action_onboarding_version": 1,
  "calendar": {"used_at": null, "seen_count": 1, "dismissed_until": null},
  "like": {"used_at": null, "seen_count": 0, "dismissed_until": null},
  "share": {"used_at": null, "seen_count": 0, "dismissed_until": null},
  "last_hint_session_id": "uuid"
}
```

Rules:

- successful action permanently suppresses its onboarding;
- dismiss starts a cooldown (initial proposal: 14 days);
- no action receives more than two unsolicited impressions;
- only one unsolicited hint per session;
- authenticated state may merge local history conservatively, keeping the strongest suppression/used signal;
- do not store this product-personalization state in the canonical Fly event database.

## Display patterns researched on Pinterest

The 2026-07-11 research pass collected and visually reviewed `80` candidates across `12` query families; `12` were shortlisted. Collection metadata lives at:

`/home/dev/projects/pinterest-idea-library/collections/20260711-mobile-contextual-onboarding-coachmark-action-icons-20260711/pins.json`

Active patterns in the lab:

1. **O1 Below-row playful callout — recommended for calendar.** The action row keeps its exact position; a light callout opens below it and points upward to the calendar icon. The rest of the page remains interactive.
2. **O2 Inline contextual strip — suitable for like.** Carries slightly longer value copy; includes the same heart icon to preserve a visible connection to the target.
3. **O3 Micro-label + halo — suitable for share/returning users.** Lowest visual noise but insufficient for teaching the compound calendar-follow outcome.
4. **Toast — post-action confirmation only.** It confirms what happened; it is not the first-use explanation.

Rejected:

- full-screen onboarding carousels;
- hints inserted above the icon row that move the taught controls downward;
- dark/high-contrast onboarding cards that visually imitate the page’s main decision blocks;
- dimmed spotlight overlays;
- three simultaneous bubbles;
- animated pulsing that persists after the first impression;
- hints on every page view;
- tooltip copy that explains only the icon name instead of user value.

## Recommended copy

Calendar callout (the action name must stay exact):

> **Добавить в календарь**
>
> .ics откроется сразу. После входа сохраним событие на сайте и пришлём изменения на почту.

The real-event trial must use truthful current-state copy instead of the target promise:

> **Добавить в календарь**
>
> Сейчас откроется .ics. Сохранение на сайте и письма об изменениях — следующий этап.

The personal-section product name is not fixed yet. Candidates are `Мои события` and the conventional `Избранное`. Do not silently rename the action itself and do not hard-code either section name into the first-use callout until the navigation naming decision is accepted.

Like inline hint:

> Нравится? Лайк улучшает ваши рекомендации и поддерживает событие.

Share micro-label / expanded hint:

> Отправить друзьям

Expanded share explanation, if needed:

> Откроется привычное меню телефона; ссылка и карточка события уже готовы.

## Accessibility and interaction

- Icon controls keep at least a `44×44px` target; current lab controls are `48px` high.
- Persistent first-use education uses `role="note"`, not a hover-only tooltip.
- The target control uses `aria-describedby` while its hint is visible.
- Dismiss has an explicit accessible label and never triggers the action.
- Keyboard focus is not moved into the hint automatically.
- The icon row never changes position when the hint appears; the hint consumes space only below it.
- The hint does not block the primary ticket CTA or sibling action controls.
- No essential meaning depends on animation; reduced-motion users see the same static hierarchy.

## Real-event preview behavior

- Build flag: `PUBLIC_EVENT_PAGE_DECISION_VARIANT=ticket-cluster`.
- Display mode: `PUBLIC_EVENT_ACTION_ONBOARDING_MODE=off|adaptive|calendar-first`; the default is `off`. `adaptive` waits until at least a later event-page view, while `calendar-first` explicitly allows the first stable eligible row.
- Review-only override: `?onboarding=calendar` forces the calendar hint without mutating impression/cooldown state.
- Normal eligibility waits until the row is at least about `72%` visible, then holds it stable for about `950ms`.
- Successful calendar/share/like use stores `used_at`; dismiss suppresses that action for `14` days; calendar receives at most two unsolicited impressions.
- The hint opens below the row. Dismissal removes only the hint and target halo; absolute row coordinates must remain unchanged.
- Share/like show truthful numeric zero in this trial so the icon row remains spatially stable. Calendar has no persisted public count field and therefore shows the shared calendar SVG with no `+` glyph and no fabricated social proof.

## Production rollout dependencies

Before moving this behavior from lab into `EventHero.astro`:

1. define and deploy private saved/follow state and a readable «Мои события» surface;
2. integrate calendar click with `.ics` plus authenticated follow without delaying the file action;
3. verify explicit email consent, outbox idempotency, real Postbox sending and delivery/suppression telemetry outside dry-run;
4. persist or safely merge onboarding state for authenticated users in personalization Supabase;
5. keep anonymous fallback local and consent-gated;
6. add analytics for `hint_eligible`, `hint_shown`, `hint_dismissed`, `action_completed_after_hint`, without a high-volume telemetry firehose;
7. test all partial-success outcomes and never claim an email/reminder that was not actually queued/sent.
