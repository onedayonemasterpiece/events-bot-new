# Static Event Page Interface References

> **Status:** reference board for product/UI design, not a proof of usability.
>
> Use this document to compare page mechanics for `kenigevents.ru` event detail pages and continuation blocks. Do not copy visual style blindly: each reference must be checked against our mobile/desktop content density, Russian event catalog, SEO and consent constraints.

## Practical shortlist for first prototype

| Reference | Surface to inspect | Borrow | Do not borrow | Mobile relevance | Desktop relevance |
| --- | --- | --- | --- | --- | --- |
| [Ticketmaster](https://www.ticketmaster.com/) | event detail / purchase flow | strong date-place-price-CTA hierarchy, sticky purchase intent | heavy ticketing friction and ads | CTA clarity | right rail/sticky CTA |
| [Timepad](https://timepad.ru/) | Russian event detail / registration | organiser/date/place/registration conventions, Russian copy patterns | platform-specific account/ticket recovery complexity | familiar local registration page | familiar local detail structure |
| [Afisha](https://www.afisha.ru/) | city/event/category discovery | dense Russian-language category/date/price scanning | noisy ad-heavy density | category/date chips | grid/list density and filters |
| [KudaGo](https://kudago.com/) | editorial city guide | editorial tone, city/category/event descriptions | broad city-guide scope beyond events MVP | readable cards | city-category navigation |
| [Bandsintown](https://www.bandsintown.com/) | concert detail / venue continuation | “more at venue” and follow/artist continuation idea | artist-follow social graph for anonymous MVP | compact continuation | venue/right-rail patterns |
| [Songkick](https://www.songkick.com/) | concert detail / related upcoming events | related upcoming events and venue/lineup context | artist fandom assumptions for all events | simple related lists | compact related/nearby modules |
| [Resident Advisor](https://ra.co/events) | music events discovery | “for you/new/picks” taxonomy and dense event filters | nightlife-specific visual language as generic default | future feed tabs | filter/category density |
| [DICE](https://dice.fm/) | mobile-first ticketing/discovery | clean mobile card rhythm and purchase-first UX | app-only/social assumptions | mobile card flow | limited, use sparingly |

## Event detail page references

| Reference | Surface | Borrow | Do not borrow | Mobile relevance | Desktop relevance |
| --- | --- | --- | --- | --- | --- |
| Ticketmaster | event detail page | event facts and CTA above fold | ticket marketplace clutter | high | high |
| Eventbrite | event detail / organiser / agenda | description, organiser, FAQ/agenda-like sections | mandatory platform signup assumptions | high | medium |
| Timepad | event detail / registration | Russian event facts and CTA language | platform-specific account flows | high | high |
| Afisha | event/category | date/category/price density | ad pressure | medium | high |
| KudaGo | event detail / list | editorial copy and city-guide navigation | broad editorial scope in MVP-0 | medium | high |
| Meetup | event/community page | organiser/community context and attendance intent | social attendance as anonymous signal | medium | medium |
| TodayTix | theatre/show page | theatre-specific urgency/availability mechanics | discount/rush mechanics until supported | high for theatre | medium |

## Continuation block references

| Reference | Continuation mechanic | Borrow | Do not borrow | Mobile relevance | Desktop relevance |
| --- | --- | --- | --- | --- | --- |
| Bandsintown | more concerts at venue/artist | “ещё на этой площадке” | follow graph in MVP-0 | high | high |
| Songkick | related upcoming events / fans-of | compact related upcoming events | artist fandom as universal model | high | high |
| RA | for you / new / picks | future feed tabs and editorial picks | nightlife-only ranking semantics | medium | high |
| Eventbrite discover | personalized feed + filters | future mobile discovery combination | app marketplace complexity in event detail MVP | high | medium |
| Fever | nearby/tailored experiences | “рядом/похоже” consumer language | aggressive marketplace urgency by default | high | medium |
| TripAdvisor / Viator / GetYourGuide | travel/activity detail | trust blocks, cancellation/booking information when available | review/travel assumptions for all local events | medium | medium |
| Airbnb Experiences | host-led experiences | tours/workshops/classes framing | social host marketplace as base model | medium | low |

## Local/Russian competitor scan

| Reference | Surface | Borrow | Do not borrow | Mobile relevance | Desktop relevance |
| --- | --- | --- | --- | --- | --- |
| Afisha Калининград pages | local category/date discovery | city/category/date expectations | ad-heavy density | medium | high |
| Timepad Kaliningrad pages | local event/registration pages | registration copy, organiser/source conventions | Timepad account coupling | high | high |
| KudaGo local/city patterns | editorial city discovery | readable cards and city rubrics | broad non-event content unless explicitly routed | medium | high |

## Design implications for `kenigevents.ru`

- Mobile related block is a vertical continuation module after event content, not an infinite homepage feed.
- Desktop should use a native grid/right-rail/module layout; do not stretch mobile cards to full width.
- Primary ticket/registration CTA stays above related recommendations.
- Related cards need reason chips, but reason chips must be compact and non-creepy.
- If local rerank finishes after the block is already visible, do not jump/reorder the visible cards; apply rerank to later chunks or show a soft “подборка обновлена” cue.
- Static fallback must be useful without JS, consent, localStorage or Supabase.
