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
