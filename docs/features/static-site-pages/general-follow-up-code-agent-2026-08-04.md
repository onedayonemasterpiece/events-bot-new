# Генеральная доработка статического сайта — документ заменён

Эта версия handoff больше не используется.

Актуальный полный prompt для кодового агента:

[`general-follow-up-code-agent-v3-2026-08-04.md`](./general-follow-up-code-agent-v3-2026-08-04.md)

Он учитывает последние решения владельца:

- сайт фокус-группы доступен без авторизации;
- feedback-блок видим, но его controls заблокированы до email/Яндекса;
- invite/share/QR остаются доступны без входа;
- добавляется рабочий `/profil/`;
- P13N-00 не переделывается, следующий slice local-first и zero-backend;
- remote personalization writes и Supabase profile cache запрещены;
- первая коллекция состоит ровно из 7 готовых артефактов;
- итогом должна быть stable immutable review candidate после terminal GitHub Actions PASS.
