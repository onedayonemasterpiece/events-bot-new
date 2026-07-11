from __future__ import annotations

import os

from .providers.notisend import NotiSendConfig
from .providers.postbox import PostboxConfig


def _flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off", ""}:
        return False
    raise ValueError(f"{name} must be a boolean flag")


def postbox_config_from_env() -> PostboxConfig:
    return PostboxConfig(
        enabled=_flag("POSTBOX_EMAIL_ENABLED", False),
        dry_run=_flag("POSTBOX_EMAIL_DRY_RUN", True),
        endpoint=os.getenv("POSTBOX_EMAIL_ENDPOINT", PostboxConfig.endpoint),
        iam_token=os.getenv("POSTBOX_EMAIL_IAM_TOKEN", ""),
        from_email=os.getenv("POSTBOX_EMAIL_FROM", PostboxConfig.from_email),
        from_name=os.getenv("POSTBOX_EMAIL_FROM_NAME", PostboxConfig.from_name),
        reply_to=os.getenv("EMAIL_REPLY_TO", PostboxConfig.reply_to),
        configuration_set=os.getenv("POSTBOX_EMAIL_CONFIGURATION_SET", ""),
    )


def notisend_config_from_env() -> NotiSendConfig:
    return NotiSendConfig(
        enabled=_flag("NOTISEND_EMAIL_ENABLED", False),
        dry_run=_flag("NOTISEND_EMAIL_DRY_RUN", True),
        endpoint=os.getenv("NOTISEND_EMAIL_ENDPOINT", NotiSendConfig.endpoint),
        api_token=os.getenv("NOTISEND_EMAIL_API_TOKEN", ""),
        from_email=os.getenv("NOTISEND_EMAIL_FROM", NotiSendConfig.from_email),
        from_name=os.getenv("NOTISEND_EMAIL_FROM_NAME", NotiSendConfig.from_name),
        reply_to=os.getenv("EMAIL_REPLY_TO", NotiSendConfig.reply_to),
    )
