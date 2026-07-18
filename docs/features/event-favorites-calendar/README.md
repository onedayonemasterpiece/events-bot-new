# Event favorites, calendar counter and «Мои события»

> Status: **product/data design; ICS preview already implemented**.
> Durable saved-event state, the unique calendar-save counter and the personal
> «Мои события» page are not implemented in production.

## Product decision

The product has three related but non-equivalent facts:

1. **ICS request/download** — the browser requested a calendar file. The same person
   may request it repeatedly, and a successful response does not prove that an OS
   calendar actually imported it.
2. **Saved to calendar through KenigEvents** — an idempotently accepted user action
   for one event occurrence. This owns the durable state and the unique counter.
3. **Favorite** — an explicit watch-list choice. It does not imply an external
   calendar import or email consent.

The public/product wording **«Добавили в календарь»** means distinct trusted
profiles whose first calendar-save action was accepted by KenigEvents. It must not
be documented as proof that Apple/Google/Yandex Calendar completed an import.
Internal metric name: `unique_calendar_saver_profiles`.

## Idempotent counter contract

### Identity and counting unit

- counting key: one trusted `profile_id` + one canonical occurrence `event_id`;
- a core event row represents the occurrence; linked dates/times remain separate
  occurrences and therefore separate saved objects;
- an authenticated Supabase identity or a consented, server-materialized anonymous
  profile may contribute to the trusted counter;
- a raw browser `anon_id`, IP, cookie or ICS URL request alone is not proof of one
  person and cannot increment the trusted unique counter;
- one person using several unlinked devices may still appear as several profiles.
  The metric must be called «unique profiles» internally, not «unique humans».

### First save, repeat and merge

The first accepted calendar save writes `first_calendar_saved_at`. Repeated clicks,
page reloads, concurrent tabs, RPC retries, another ICS download, remove/re-add and
the same `action_id` do not increment the counter again. They may update
`last_calendar_requested_at` and bounded operational evidence.

Anonymous-to-authorized linking is transactional. If both source and target
profiles already saved the same occurrence, they collapse to one owner/occurrence
fact and the aggregate is reconciled rather than preserving a double count.
Rescheduling keeps the same event identity and saved state. A confirmed event merge
migrates the losing id to the winner and performs the same deduplication.

Removing an event from «Мои события» changes current membership but does not
erase the historical first-save fact and does not decrement the cumulative counter.
Account deletion removes profile-owned rows; only an irreversibly aggregated
counter may remain under the accepted retention/privacy policy.

### Download is a separate measurement

`ics_request`/`ics_download` is optional short-retention operational analytics. It
may count attempts, but it never feeds the unique calendar-saver counter, the
navigation badge or favorites. Transport-leg ICS requests use their own action kind
and never become an event save implicitly.

## Interaction flow

The static calendar link remains a normal crawlable `.ics` URL and works without
JavaScript or when personalization/Supabase is unavailable.

With JavaScript and a trusted/materialized actor:

1. the calendar CTA keeps the direct ICS URL as its real `href`;
2. progressive enhancement submits an idempotent `calendar_save_v1` mutation with
   `action_id`, `event_id`, occurrence revision and surface;
3. the browser still opens the ICS even when the mutation times out;
4. only a successful server response changes UI to **«В моём календаре»**,
   updates «Мои события» and returns the reconciled aggregate count;
5. failure copy says that the calendar file is still available but cross-device
   saved state was not confirmed. The UI must not claim a successful save.

For a local-only visitor, the site may remember the selection on that device and
offer **«Войти, чтобы не потерять на другом устройстве»**. Local state
enters the server counter only after secure materialization/linking and deduplication.

The browser-to-OS boundary cannot be atomic: KenigEvents can prove its accepted
save action and delivery of the ICS, but not the user's final tap inside an external
calendar application.

## Supabase/Postgres model

Canonical event facts stay in Fly SQLite. Personalization Supabase/Postgres owns
private per-profile state and a bounded public aggregate.

### Private current/once state

Logical table `private.user_event_state` (exact migration names may differ):

| Field | Contract |
|---|---|
| `profile_id` | durable owner mapped to the current Supabase identity |
| `event_id` | canonical occurrence id from Fly SQLite projection |
| `favorite_state` | current explicit favorite membership |
| `calendar_state` | current KenigEvents calendar membership |
| `first_calendar_saved_at` | immutable first accepted calendar save |
| `last_calendar_requested_at` | last accepted calendar action, not a counter source |
| `favorite_updated_at`, `calendar_updated_at` | conflict resolution/versioning |
| `event_revision_at_save` | lifecycle/reconciliation evidence |
| `created_at`, `updated_at` | audit/operations timestamps |

Required constraint: one row per `(profile_id, event_id)`. UI removal toggles state;
it does not delete `first_calendar_saved_at`. Mutations require an idempotency key,
short transaction and server validation that the event/occurrence exists in the
current trusted projection.

### Public aggregate

`public.personalization_event_reaction_counter` gains a server-maintained
`calendar_savers_count`. Browser roles may read the aggregate only; they cannot
write it or select profile state. The value is folded/reconciled from private
owner/occurrence facts, not incremented from a client-supplied number.

Preferred mutation/read path is a same-origin API that calls private SQL/RPC. If a
Data API RPC is deliberately exposed, it must revoke default `PUBLIC` execute,
grant only the required role, validate `auth.uid()`/profile ownership and use RLS or
an equally strict server-owned authorization boundary. Direct browser writes to
the aggregate or private table are forbidden.

### Required operations

- `calendar_save_v1(event_id, action_id, occurrence_revision, surface)`;
- `favorite_set_v1(event_id, desired_state, action_id, surface)`;
- `my_events_list_v1(cursor, lifecycle_filter, kind_filter)` as one batch read;
- `my_events_remove_v1(event_id, remove_calendar, remove_favorite, action_id)`;
- merge/reconciliation operation for anonymous→authorized identity and event merge;
- aggregate rebuild/check that can derive the public count from canonical private
  state and detect drift.

## Global menu and «Мои события» page

### Naming and route

- mobile navigation label: **«Моё»**;
- desktop navigation and accessible name: **«Мои события»**;
- canonical personal hub: `/moi-sobytiya/`;
- the previously planned `/izbrannoe/` becomes a compatibility route to the same
  privacy-safe shell with the «Избранное» filter selected; it is not a second
  store or a separately cached personal page.

The menu badge is the distinct count of **current upcoming rows in the union** of
calendar and favorite state. An event marked both ways counts once. Past, removed,
merged-away and transport-only rows do not inflate it; cancelled rows remain visible
with status until the user acknowledges/removes them but do not look actionable.

### Information architecture

The page heading is **«Мои события». It has one loaded list and client-side
filters, not three independently fetched pages:

- **Предстоящие** — default chronological view, grouped by `Сегодня`,
  `Завтра`, `На этой неделе` and later months;
- filter **«В календаре»** — rows with current calendar state;
- filter **«Избранное»** — rows with explicit favorite state;
- **Прошедшие** — collapsed archive, newest first;
- a top **«Изменения»** block appears only when saved events were
  rescheduled, cancelled or merged and require attention.

Reference mobile composition (exact tokens/components are selected during the
shared design-system freeze):

```text
┌─ Мои события ──────────────┐
│ 3 предстоящих                     │
│ [Предстоящие]  [Прошедшие]       │
│ [Все 3] [В календаре 2] [Избранное 2] │
│                                      │
│ Изменения                            │
│ ⚠ Начало перенесено на 19:30       │
│                                      │
│ Сегодня                              │
│ 18:00  Название события              │
│        Место · 12+                   │
│        [В календаре] [В избранном] │
│        [Открыть] [Ещё раз .ics]       │
└──────────────────────────────────────┘
```

Desktop keeps the same information order and filters, with a wider two-column
card/list layout only when it preserves chronological scanning; it does not invent
a different information architecture.

Each row shows the event title, exact occurrence date/time, place, lifecycle status,
age/admission facts and state markers **«В календаре»** / **«В избранном»**.
Actions are occurrence-specific: open event, request ICS again, toggle favorite and
remove one or both KenigEvents states.

Removing calendar state must say: **«Мы уберём событие из „Моих
событий“, но не можем удалить его из вашего Apple/Google/Яндекс
Календаря»**. If favorite remains active, the row remains under that filter.

### Privacy, loading and fallback

- `/moi-sobytiya/` and `/izbrannoe/` are `noindex` static shells with no personal
  rows, counts, identity or email in CDN HTML/cache;
- one authenticated/RLS-protected batch loads the list; no per-card remote loop;
- loading does not flash another/previous user's count or rows;
- account switch clears the previous projection before loading the next owner;
- backend failure keeps static navigation and direct ICS working, shows an explicit
  degraded state and never claims an unconfirmed mutation;
- local-only state is visibly labelled **«Только на этом устройстве»**;
- empty states distinguish «nothing saved yet», filter-empty and backend error.

## Email and notification boundary

Calendar/favorite state never implies recommendation or transactional-email
consent. After a save, a separate control may offer a D-1 reminder and must show
the masked verified destination and explicit purpose. Removing a reminder does not
remove the event; removing an event cancels future event reminder state according
to the transactional-email contract but cannot remove an external calendar entry.

## Acceptance summary

- five repeat ICS requests by one profile produce one first-save fact and a `+1`
  aggregate change, not `+5`;
- two concurrent tabs and a retried `action_id` converge to one state;
- anonymous→authorized and event-merge reconciliation remove double ownership;
- another occurrence of the same programme is a separate save;
- direct/no-JS ICS stays usable when the save backend fails and is not falsely
  reported as a confirmed server save;
- «Мои события» renders union/filter/lifecycle states from one protected batch,
  with a deduplicated upcoming badge and explicit external-calendar removal copy.

## Related documentation

- [Personalization data ownership](../../architecture/personalization-data-ownership.md)
- [Site user identity](../site-user-identity/README.md)
- [Static event pages](../static-site-pages/README.md)
- [Static release UI contract](../static-site-pages/release-ui-contract.md)
- [Transactional event email](../event-email-notifications/README.md)
- [Email delivery](../../operations/email-delivery.md)
