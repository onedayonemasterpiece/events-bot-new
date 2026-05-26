#!/usr/bin/env python3
"""DEV-only: temporarily flip a user to partner role in a LOCAL DB copy.

Use-case: live E2E run of the partner promo flow against an isolated
``db_e2e_prod_snapshot_*.sqlite`` while keeping prod untouched. The
script writes a small JSON receipt next to the DB so the role can be
reverted exactly to its previous state when E2E is done.

Usage:
  # set role
  python scripts/dev_set_partner_role.py set \\
      --db artifacts/test-results/db_e2e_prod_snapshot_*.sqlite \\
      --user-id 185169715 \\
      --organization "Научная библиотека"

  # revert from receipt
  python scripts/dev_set_partner_role.py revert \\
      --db artifacts/test-results/db_e2e_prod_snapshot_*.sqlite
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from pathlib import Path


def _receipt_path(db_path: str) -> Path:
    return Path(db_path).with_suffix(".role_receipt.json")


def cmd_set(db_path: str, user_id: int, organization: str, username: str | None = None) -> int:
    receipt = _receipt_path(db_path)
    if receipt.exists():
        print(
            f"ERROR: receipt already exists at {receipt} — "
            "revert previous role first or delete the receipt manually.",
            file=sys.stderr,
        )
        return 2
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        row = conn.execute(
            "SELECT user_id, username, is_superadmin, is_partner, organization, blocked "
            "FROM user WHERE user_id = ?",
            (int(user_id),),
        ).fetchone()
        if row is None:
            # User does not exist yet in the DB snapshot. For E2E we create
            # a row up-front; the receipt records that creation so revert
            # deletes the user entirely, leaving the snapshot pristine.
            prev = {
                "user_id": int(user_id),
                "username": username,
                "is_superadmin": 0,
                "is_partner": 0,
                "organization": None,
                "blocked": 0,
                "_created_by_dev_set_partner_role": True,
            }
            receipt.write_text(json.dumps(prev, ensure_ascii=False, indent=2), encoding="utf-8")
            conn.execute(
                "INSERT INTO user (user_id, username, is_superadmin, is_partner, "
                "organization, blocked) VALUES (?, ?, 0, 1, ?, 0)",
                (int(user_id), username, organization),
            )
            conn.commit()
            print(
                f"OK: user_id={user_id} inserted as partner of {organization!r}"
                f" in {db_path} (was absent).\n     receipt: {receipt}",
            )
            return 0
        prev = {
            "user_id": int(row[0]),
            "username": row[1],
            "is_superadmin": int(row[2] or 0),
            "is_partner": int(row[3] or 0),
            "organization": row[4],
            "blocked": int(row[5] or 0),
        }
        receipt.write_text(json.dumps(prev, ensure_ascii=False, indent=2), encoding="utf-8")
        # Drop superadmin while testing the partner view so /promo and the
        # 🎬 button render as a partner would actually see them.
        conn.execute(
            "UPDATE user SET is_partner = 1, is_superadmin = 0, "
            "organization = ?, blocked = 0 WHERE user_id = ?",
            (organization, int(user_id)),
        )
        conn.commit()
        print(
            f"OK: user_id={user_id} is now partner of {organization!r} in {db_path}.\n"
            f"     previous state saved to {receipt}.",
        )
        return 0
    finally:
        conn.close()


def cmd_revert(db_path: str) -> int:
    receipt = _receipt_path(db_path)
    if not receipt.exists():
        print(f"ERROR: no receipt at {receipt}, nothing to revert.", file=sys.stderr)
        return 2
    prev = json.loads(receipt.read_text(encoding="utf-8"))
    conn = sqlite3.connect(db_path, timeout=30)
    try:
        if prev.get("_created_by_dev_set_partner_role"):
            conn.execute("DELETE FROM user WHERE user_id = ?", (int(prev["user_id"]),))
            conn.commit()
            receipt.unlink()
            print(
                f"OK: deleted user_id={prev['user_id']} (was inserted by this script) "
                f"from {db_path}."
            )
            return 0
        conn.execute(
            "UPDATE user SET is_superadmin = ?, is_partner = ?, organization = ?, "
            "blocked = ?, username = ? WHERE user_id = ?",
            (
                int(prev["is_superadmin"]),
                int(prev["is_partner"]),
                prev["organization"],
                int(prev["blocked"]),
                prev["username"],
                int(prev["user_id"]),
            ),
        )
        conn.commit()
        receipt.unlink()
        print(f"OK: reverted user_id={prev['user_id']} to previous state in {db_path}.")
        return 0
    finally:
        conn.close()


def main() -> int:
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="action", required=True)

    sp_set = sub.add_parser("set", help="flip user to partner role")
    sp_set.add_argument("--db", required=True)
    sp_set.add_argument("--user-id", type=int, required=True)
    sp_set.add_argument(
        "--organization",
        required=True,
        help="Organization name as stored in user.organization",
    )
    sp_set.add_argument(
        "--username",
        default=None,
        help="Telegram username (without @); used only when inserting a new row",
    )

    sp_revert = sub.add_parser("revert", help="restore previous state from receipt")
    sp_revert.add_argument("--db", required=True)

    args = ap.parse_args()
    if args.action == "set":
        return cmd_set(args.db, args.user_id, args.organization, args.username)
    if args.action == "revert":
        return cmd_revert(args.db)
    return 1


if __name__ == "__main__":
    sys.exit(main())
