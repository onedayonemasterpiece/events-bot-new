# Integration Report — official rail and multimodal directory

Branch: `integration/event-transport-schedule`
Mode: read-only parallel research, then serial integration.

| ID | Requirement | Status | Evidence |
|---|---|---|---|
| R01 | Пионерский and Зеленоградск use frequent rail as priority | Done in policy/reference; Partial in public UI | `locality_policies`; official 3-Jul coastal sources; exact-date Pionersky export remains gated |
| R02 | Балтийск uses the multi-pair summer diesel timetable as priority | Done in policy/reference; Partial in public UI | reviewed 29-Jun daily matrix with 5 outbound/6 return trips; exact-date export remains gated |
| R03 | Гвардейск, Знаменск, Черняховск, Гусев and eastern stops compare rail and bus | Done in policy/reference; Partial in public UI | reviewed 1-Jun RA-2 matrix, explicit skip lists and mixed-mode Tyunin rule |
| R04 | Железнодорожный, Багратионовск, Мамоново/Бранденбург include rail safely | Done in policy/reference; Partial in public UI | seasonal/limited patterns; Ushakovo has no direct station and requires reviewed Ladushkin road transfer |
| R05 | Краснолесье weekend/holiday train can support matching events | Done in policy/reference; Partial in public UI | exact `09:55 → 12:35`, `18:25 → 21:00`, fixed visit window and border-zone warning contract |
| R06 | Verify schedules on the carrier site | Done for source inventory and requested matrices | all 13 official index pages/assets inventoried with effective date/hash; official/API coastal minute differences recorded instead of hidden |

## Lane outcomes

All three child lanes were read-only and made no repository writes. Their evidence was reconciled into one integrator-owned JSON contract. No lane was abandoned or superseded.

## Integration evidence

- `site/src/data/railRouteDirectory.json`
- `site/scripts/check-rail-transport-directory.mjs`
- `docs/features/static-site-pages/rail-multimodal-directory.md`
- presentation checklist and `TD-STATIC-TRANSPORT-001` acceptance expansion

Public rendering outside Светлогорск/Зеленоградск remains intentionally disabled until exact-date calendars, exceptions and last-mile access are exported atomically. This prevents directory presence from creating false journey suggestions.

## Follow-up — ДС «Янтарный» / Елизаветинская

The 2026-07-12 product follow-up is **Done in reference / Partial in public UI**. `о.п. Елизаветинская` is worthwhile for this venue: the reviewed pedestrian route is about `627 m / 8–10 min`, and the current official matrix has regular non-express stops. A dedicated `venue_specific_optional` route and exact venue aliases/address were added. Its display contract shows the same train once with clickable Южный (`15–18 min`) and Северный (`7–8 min`) choices, plus the official 2026 `35 ₽` up-to-10-km fare. Public activation remains gated by an exact-date calendar export and a real-event regression; no other Kaliningrad event can inherit the route by city alone.
