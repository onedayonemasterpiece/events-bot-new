# F18 desktop clipboard manual matrix — template v1

> Evidence version: `f18-desktop-clipboard-matrix-v1`
> RC SHA: `<IMPLEMENTATION_SHA>`
> Preview: `<PUBLIC_PREVIEW_URL>`
> Manifest SHA-256: `<SERVICE_SHARE_MANIFEST_SHA256>`
> Status: **Pending — no native Windows/macOS results have been performed or inferred**.

## Rules

- Record exact OS, source browser and target app versions.
- Run D0, D1 and D2 twice in a clean test document/conversation (`2/2`).
- Record one primary taxonomy result from the canonical research contract.
- Record whether the link is clickable, image appears, CTA/domain is visible and
  image is attachment/inline/remote HTML.
- Attach a redacted screenshot; never include personal chats, clipboard contents,
  full UA, account/profile/session identifiers or unrelated browser data.
- Playwright/mock results do not populate this matrix.

## Windows 11

| Source browser | Target | D0 | D1 | D2 | Link clickable | Image/CTA | Repeat | Evidence |
|---|---|---|---|---|---|---|---|---|
| Edge `<version>` | controlled textarea | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Edge `<version>` | controlled contenteditable/image target | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Edge `<version>` | Notepad | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Edge `<version>` | Telegram Desktop | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Edge `<version>` | VK web composer | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Edge `<version>` | MAX web/desktop | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Edge `<version>` | Word/Outlook or substitute | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Chrome `<version>` | controlled + Notepad + Telegram + VK + rich editor | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Firefox `<version>` | controlled + Notepad + Telegram + one web composer | Pending | Pending | Pending | Pending | Pending | Pending | Pending |

## macOS

| Source browser | Target | D0 | D1 | D2 | Link clickable | Image/CTA | Repeat | Evidence |
|---|---|---|---|---|---|---|---|---|
| Safari `<version>` | controlled textarea | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Safari `<version>` | controlled contenteditable/image target | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Safari `<version>` | TextEdit plain/rich | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Safari `<version>` | Notes | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Safari `<version>` | Telegram Desktop | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Safari `<version>` | VK web composer | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Safari `<version>` | MAX web/desktop | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Safari `<version>` | Pages/Mail or substitute | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Chrome `<version>` | controlled + TextEdit + Notes + Telegram + VK + rich editor | Pending | Pending | Pending | Pending | Pending | Pending | Pending |
| Firefox `<version>` | controlled + TextEdit + Telegram + one web composer | Pending | Pending | Pending | Pending | Pending | Pending | Pending |

## Mobile companion matrix

These checks are part of full F18, not the desktop clipboard decision.

| OS/device/browser | Telegram file/text fallback | VK file/text fallback | MAX file/text fallback | Return to browser | Evidence |
|---|---|---|---|---|---|
| Android `<device/OS/browser>` | Pending | Pending | Pending | Pending | Pending |
| iOS `<device/OS/browser>` | Pending | Pending | Pending | Pending | Pending |

## Owner decision

- Selected desktop production mode: `Pending (D0 remains default)`
- Accepted browser policy: `Pending`
- Accepted labels/success/fallback: `Pending`
- Owner/date: `Pending`
