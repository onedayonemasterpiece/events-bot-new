"""Partner-promo FSM session state.

The full creation flow lives in ``handlers/partner_promo_cmd.py``; this
module only owns the dataclass and the TTL session caches so the handler
can keep imports thin.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Optional

from cachetools import TTLCache


PARTNER_PROMO_INPUT_COUNT = "count"
PARTNER_PROMO_INPUT_END_DATE = "end_date"
PARTNER_PROMO_INPUT_DISCLOSURE = "disclosure"
PARTNER_PROMO_INPUT_RENAME = "rename"


@dataclass
class PartnerPromoSession:
    """Mutable state for one in-progress partner-promo creation flow."""

    event_id: int
    step: int = 1
    surface: Optional[str] = None
    profile_key: Optional[str] = None
    slot_policy: Optional[str] = None
    count: Optional[int] = None
    ends_at: Optional[date] = None
    is_editorial: bool = False
    sponsorship_disclosure: Optional[str] = None
    title_override: Optional[str] = None
    info_message_id: Optional[int] = None
    # When set, the FSM is in "add activity to an existing campaign" mode —
    # period/mode/disclosure come from the campaign, FSM jumps from step 3
    # (count) straight to step 6 (confirm).
    add_to_campaign_id: Optional[int] = None


@dataclass
class PartnerPromoInputSession:
    """User reply waited-for state — links a chat reply to a session field."""

    event_id: int
    field: str
    campaign_id: Optional[int] = None


partner_promo_sessions: TTLCache[int, PartnerPromoSession] = TTLCache(maxsize=128, ttl=30 * 60)
partner_promo_input_sessions: TTLCache[int, PartnerPromoInputSession] = TTLCache(
    maxsize=128, ttl=10 * 60
)
