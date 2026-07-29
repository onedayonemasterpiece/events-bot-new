# Focus auth product fixes execution matrix

| ID | Requirement | Area | Likely files | Dependencies | Conflict risk | Lane | Parallelizable? | Done when |
|---|---|---|---|---|---|---|---|---|
| R01 | Повторная отправка OTP и основные ошибочные состояния должны быть понятны и работоспособны | Email auth UI | `FocusGroupInviteIntake.astro`, OTP tests | Hosted OTP contract | High | integrator | No | Immediate busy/success/error feedback, visible code form, cooldown resend, changed-email and retry behavior tested |
| R02 | Установленное PWA должно называться обычным «Анонсы» и оставаться полезным после исследования | PWA | focus/root manifests, invite page, install copy | R03 | High | integrator | No | Focus invite installs the permanent product identity/name/icon/start route |
| R03 | После 30 дней человек остаётся обычным пользователем без повторного подтверждения уже сделанных выборов | Lifecycle/consent | focus storage/runtime, onboarding copy, docs | R02 | High | integrator | No | Research period ends independently; account, product settings, saves and explicit continuing consent remain |
| R04 | Интерфейс email должен сразу реагировать и показывать поле кода | Email auth UI | `FocusGroupInviteIntake.astro`, browser tests | R01 | High | integrator | No | Pressing send immediately announces progress and successful delivery reveals/focuses OTP |
| R05 | Залогиненный пользователь должен видеть доступный выход без поиска подвала | Shared auth UI | invite component, shared auth runtime/tests | R04 | Medium | integrator | No | Signed-in state shows name + visible logout next to auth controls; methods return after logout |
| R06 | Текст должен говорить с новым пользователем простыми словами и объяснять роль подтверждения для розыгрыша | Product copy/docs | invite/PWA copy, canonical docs | R01–R05 | Medium | integrator | No | No implementation jargon in user-facing copy; optional verification and prize value are explicit |
| R07 | Опубликовать проверяемую noindex-версию | Build/release | preview build, gates, immutable deploy | R01–R06 | High | integrator | No | Exact SHA is built, checked and available at one phone/desktop URL |
| R08 | QR → установка/отказ → запуск приложения/сайта → подтверждение/пропуск должен быть одним последовательным сценарием | Onboarding routing | invite intake, manifest, root resume | R02, R04 | High | onboarding-map + integrator | Read-only mapping only | At most one stage is visible; fresh PWA launch resumes `joining`; completion opens ordinary home |
| R09 | Оценка 0–10 и сообщение о проблеме со скриншотом находятся внизу каждой основной страницы | Shared feedback UI | `EventLayout`, Lab panel, route-family mapper | R03, R08 | High | feedback-map + integrator | Read-only mapping only | Active focus marker reveals one panel on every principal page family without horizontal overflow |
| R10 | Feedback должен сохраняться приватно и не приниматься от анонимного браузера | Supabase/Postgres/Storage | migration, RPC, Storage RLS, SQL contract | R09 | High | integrator | No | Authenticated owner can submit; anon cannot call RPC/read; screenshots are private and owner-scoped |
| R11 | Активный участник может показать тот же массовый QR или поделиться одной ссылкой | Invite sharing | Lab panel, existing QR generator | R08, R09 | Medium | integrator | No | All share surfaces emit the same exact fragment invite URL |
| R12 | Все исследовательские функции визуально отмечены колбой и `Lab`, без служебных терминов | Product copy/UI | badge, onboarding, feedback panel | R06, R09 | Medium | copy-review + integrator | Read-only review only | Public copy passes the jargon audit and focus-only controls carry the shared badge |
| R13 | Проблему отсутствующего hero-talk в установленном приложении не потерять, но не задерживать запуск фокус-группы | Release debt | presentation checklist | R08 | Low | integrator | No | P0 follow-up is recorded with fresh-install reproduction gate |

## Closure

| ID | Status | Evidence |
|---|---|---|
| R01–R06 | Done | Existing OTP/auth product suite remains green; hosted template and six-digit dual-path contract unchanged |
| R08 | Done | Local mobile browser: QR stores `joining`; manifest root `?launch=pwa` resumes at identity; skip activates and opens ordinary home |
| R09 | Done | Mobile browser checked home, calendar, weekend, Popular, Search, collection, festivals, clubs, exhibitions, unusual, favorites, “Для меня” and event detail; panel visible, no horizontal overflow |
| R10 | Done | Live migration applied; transactional SQL contract passed; anonymous RPC probe rejected |
| R11 | Done | One stable mass token is reused by owner and per-page share surfaces |
| R12 | Done | Read-only UX review applied; visible public copy avoids implementation vocabulary |
| R13 | Done | `TD-PRESENTATION-UI-002` recorded as post-launch P0 |
| R07 | Done | Immutable noindex candidate `preview-20260729-focus-simple-r15-a5cc0256` published; hosted mobile/desktop E2E passed; exact QR decoded before and after Telegram delivery in thread `548` |
