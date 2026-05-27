"""Per-kernel-slug accelerator preference with randomized TTL.

When a Kaggle batch run sits in QUEUED for too long (default 5 min), the
poller "demotes" that slug to a faster-to-schedule accelerator tier (P100 →
T4 → fail). The demotion is recorded as a `Setting` row keyed by the resolved
Kaggle slug; subsequent pushes read it and override `acc=` in
`api.kernels_push`. TTL is randomized in 18-30h so the slot that paid the
5-min queue cost on demote day doesn't pay it again on the same cron the
next day — expiry drifts across days and the cost averages across slots.
"""

from __future__ import annotations

import json
import logging
import os
import random
from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import select

from db import Database
from models import Setting

logger = logging.getLogger(__name__)

PREF_KEY_PREFIX = "kaggle_accel_pref:"

TIER_DEFAULT = "p100"
TIER_T4 = "t4x2"

_LADDER: dict[str, list[str]] = {
    "zigomaro/cherryflash": [TIER_DEFAULT, TIER_T4],
    "zigomaro/koenigsberg-stories": [TIER_DEFAULT, TIER_T4],
}

_TIER_TO_MACHINE_SHAPE = {
    TIER_DEFAULT: "NvidiaTeslaP100",
    TIER_T4: "NvidiaTeslaT4",
}


def _ttl_hours_range() -> tuple[float, float]:
    lo = float(os.getenv("KAGGLE_ACCEL_PREF_TTL_HOURS_MIN", "18") or 18)
    hi = float(os.getenv("KAGGLE_ACCEL_PREF_TTL_HOURS_MAX", "30") or 30)
    if lo <= 0 or hi <= 0 or lo > hi:
        return 18.0, 30.0
    return lo, hi


def queue_demote_threshold_sec() -> int:
    raw = os.getenv("KAGGLE_QUEUE_DEMOTE_THRESHOLD_SEC", "300") or "300"
    try:
        v = int(raw)
        return v if v > 0 else 300
    except ValueError:
        return 300


class AccelPref:
    __slots__ = ("tier", "expires_at", "reason")

    def __init__(self, tier: str, expires_at: datetime, reason: str = "") -> None:
        self.tier = tier
        self.expires_at = expires_at
        self.reason = reason

    def to_json(self) -> str:
        return json.dumps(
            {
                "tier": self.tier,
                "expires_at": self.expires_at.astimezone(timezone.utc).isoformat(),
                "reason": self.reason,
            },
            ensure_ascii=False,
        )

    @classmethod
    def from_json(cls, raw: str) -> Optional["AccelPref"]:
        try:
            data = json.loads(raw)
            tier = str(data.get("tier") or "").strip()
            exp_raw = str(data.get("expires_at") or "").strip()
            if not tier or not exp_raw:
                return None
            exp = datetime.fromisoformat(exp_raw)
            if exp.tzinfo is None:
                exp = exp.replace(tzinfo=timezone.utc)
            return cls(tier=tier, expires_at=exp, reason=str(data.get("reason") or ""))
        except (ValueError, TypeError, KeyError):
            return None


def _setting_key(slug: str) -> str:
    return f"{PREF_KEY_PREFIX}{slug}"


async def read_active_pref(db: Database, slug: str) -> Optional[AccelPref]:
    """Return the current pref for `slug` if set and not expired, else None."""
    if not slug:
        return None
    async with db.get_session() as session:
        row = await session.get(Setting, _setting_key(slug))
        if not row or not row.value:
            return None
    pref = AccelPref.from_json(row.value)
    if pref is None:
        return None
    if datetime.now(timezone.utc) >= pref.expires_at:
        return None
    return pref


async def write_pref(db: Database, slug: str, pref: AccelPref) -> None:
    async with db.get_session() as session:
        key = _setting_key(slug)
        row = await session.get(Setting, key)
        if row is None:
            row = Setting(key=key, value=pref.to_json())
            session.add(row)
        else:
            row.value = pref.to_json()
            session.add(row)
        await session.commit()


def next_tier(slug: str, current: str) -> Optional[str]:
    ladder = _LADDER.get(slug)
    if not ladder:
        return None
    try:
        idx = ladder.index(current)
    except ValueError:
        return None
    if idx + 1 >= len(ladder):
        return None
    return ladder[idx + 1]


def random_ttl() -> timedelta:
    lo, hi = _ttl_hours_range()
    return timedelta(hours=random.uniform(lo, hi))


async def demote(
    db: Database, slug: str, *, current_tier: str, reason: str
) -> Optional[AccelPref]:
    """Move slug to the next ladder tier with a random TTL. Returns None when
    the ladder is exhausted (caller should hard-fail)."""
    nxt = next_tier(slug, current_tier)
    if nxt is None:
        logger.warning(
            "kaggle_accel: ladder exhausted for slug=%s current=%s",
            slug,
            current_tier,
        )
        return None
    pref = AccelPref(
        tier=nxt,
        expires_at=datetime.now(timezone.utc) + random_ttl(),
        reason=reason,
    )
    await write_pref(db, slug, pref)
    logger.warning(
        "kaggle_accel: demoted slug=%s %s -> %s expires_at=%s reason=%r",
        slug,
        current_tier,
        nxt,
        pref.expires_at.isoformat(),
        reason,
    )
    return pref


def tier_to_machine_shape(tier: str) -> Optional[str]:
    """Map our internal tier label to Kaggle SDK `machine_shape` string."""
    return _TIER_TO_MACHINE_SHAPE.get(tier)


def ladder_for(slug: str) -> list[str]:
    return list(_LADDER.get(slug) or [])
