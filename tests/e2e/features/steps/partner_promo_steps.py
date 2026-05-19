"""Step definitions specific to the partner promo flow.

Reuses primitives from ``bot_steps.py`` (HumanUserClient is attached to
``context.client``) and adds a handful of partner-promo-specific
predicates.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from behave import then, when

logger = logging.getLogger("e2e.partner_promo")


def _last_message(context) -> Any:
    """Return the latest message captured by HumanUserClient or stored on ctx."""

    msg = getattr(context, "last_response", None)
    if msg is not None:
        return msg
    return None


def _all_buttons(message) -> list[str]:
    if not message or not getattr(message, "buttons", None):
        return []
    out: list[str] = []
    for row in message.buttons:
        for btn in row:
            out.append(btn.text)
    return out


@then('в тексте сообщения встречается "{needle}"')
def step_text_contains(context, needle: str) -> None:
    msg = _last_message(context)
    assert msg is not None, "no last message"
    text = (getattr(msg, "text", "") or getattr(msg, "raw_text", "") or "")
    assert needle in text, (
        f'expected "{needle}" in last message text, got: {text[:300]!r}'
    )


@then('под сообщением не должно быть кнопки "{btn_text}"')
def step_no_button(context, btn_text: str) -> None:
    msg = _last_message(context)
    assert msg is not None, "no last message"
    labels = _all_buttons(msg)
    assert not any(btn_text in l for l in labels), (
        f'unexpected button "{btn_text}" present, all buttons: {labels}'
    )


@when('я нажимаю инлайн-кнопку начинающуюся с "{prefix}"')
def step_click_button_prefix(context, prefix: str) -> None:
    msg = _last_message(context)
    assert msg is not None, "no last message"
    target = None
    for row in msg.buttons or []:
        for btn in row:
            if (btn.text or "").startswith(prefix):
                target = btn
                break
        if target:
            break
    assert target is not None, (
        f'button starting with "{prefix}" not found; buttons: {_all_buttons(msg)}'
    )

    async def _click() -> None:
        await target.click()

    context.loop.run_until_complete(_click())


@then('я получаю alert содержащий "{text}"')
def step_alert_contains(context, text: str) -> None:
    """Telethon-level: HumanUserClient must capture the latest alert text.

    We expose ``context.last_alert`` via human_client.py — when the bot
    answers a callback with ``show_alert=True`` it should populate this
    field. If your client does not yet capture alerts, the fallback
    asserts on ``context.last_response.text`` instead.
    """

    alert = getattr(context, "last_alert", None)
    if alert:
        assert text in alert, f"alert mismatch: {alert!r}"
        return
    msg = _last_message(context)
    fallback = (getattr(msg, "text", "") or "") if msg else ""
    assert text in fallback, (
        f'expected alert containing "{text}", got alert={alert!r}, '
        f'last text={fallback[:200]!r}'
    )


@then('последнее сообщение всё ещё содержит "{needle}"')
def step_message_still_contains(context, needle: str) -> None:
    msg = _last_message(context)
    assert msg is not None, "no last message"
    text = (getattr(msg, "text", "") or "")
    assert needle in text, (
        f'expected message to still contain "{needle}", got: {text[:200]!r}'
    )


@then('последний ответ бота закрыт или содержит "{needle}"')
def step_message_closed_or_contains(context, needle: str) -> None:
    """After Cancel the bot may either delete the message or replace text."""

    msg = _last_message(context)
    if msg is None:
        return  # deletion is acceptable
    text = (getattr(msg, "text", "") or "")
    if not text:
        return
    assert needle in text, (
        f'expected cancel response to contain "{needle}" or be deleted, '
        f'got: {text[:200]!r}'
    )
