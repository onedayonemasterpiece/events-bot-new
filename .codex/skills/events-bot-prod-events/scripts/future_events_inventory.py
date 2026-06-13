#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import json
import os
import subprocess
import sys
import textwrap


def _remote_code(target_date: str, runs: int) -> str:
    return r'''
import json, sqlite3
target_date = __TARGET_DATE__
runs_limit = __RUNS_LIMIT__
con = sqlite3.connect('/data/db.sqlite')
con.row_factory = sqlite3.Row

def rows(sql, params=()):
    return [dict(r) for r in con.execute(sql, params).fetchall()]

out = {"target_date": target_date}
out["inventory"] = {
    "active_total": con.execute(
        "SELECT COUNT(*) FROM event WHERE date=? AND lifecycle_status='active' AND silent=0",
        (target_date,),
    ).fetchone()[0],
    "with_tg_post": con.execute(
        "SELECT COUNT(*) FROM event WHERE date=? AND lifecycle_status='active' AND silent=0 AND tg_event_post_id IS NOT NULL",
        (target_date,),
    ).fetchone()[0],
    "with_kldevents_url": con.execute(
        "SELECT COUNT(*) FROM event WHERE date=? AND lifecycle_status='active' AND silent=0 AND source_vk_post_url LIKE '%wall-231920894_%'",
        (target_date,),
    ).fetchone()[0],
    "free_with_tg_post": con.execute(
        "SELECT COUNT(*) FROM event WHERE date=? AND lifecycle_status='active' AND silent=0 AND tg_event_post_id IS NOT NULL AND COALESCE(is_free,0)=1",
        (target_date,),
    ).fetchone()[0],
}
out["events_with_tg"] = rows("""
    SELECT id, title, event_type, time, location_name, is_free, tg_event_post_id,
           tg_event_post_url, source_vk_post_url, topics
    FROM event
    WHERE date=? AND lifecycle_status='active' AND silent=0 AND tg_event_post_id IS NOT NULL
    ORDER BY time, id
""", (target_date,))
out["topic_groups_with_tg"] = rows("""
    SELECT topics, COUNT(*) AS count
    FROM event
    WHERE date=? AND lifecycle_status='active' AND silent=0 AND tg_event_post_id IS NOT NULL
    GROUP BY topics
    ORDER BY count DESC, topics
""", (target_date,))
poll_runs = rows("""
    SELECT id, profile_key, run_key, status, target_event_date, question_text,
           poll_message_id, reply_message_id, forwarded_message_id,
           winner_option_id, winner_text, chosen_event_id,
           options_json, result_json, error_json, created_at
    FROM poll_repost_run
    WHERE target_event_date=?
    ORDER BY id DESC
    LIMIT ?
""", (target_date, runs_limit))
for run in poll_runs:
    for key in ("options_json", "result_json", "error_json"):
        try:
            run[key[:-5] if key.endswith("_json") else key] = json.loads(run.get(key) or "{}")
        except Exception:
            run[key[:-5] if key.endswith("_json") else key] = {}
        run.pop(key, None)
out["poll_runs"] = poll_runs
event_ids = [int(e["id"]) for e in out["events_with_tg"]]
if event_ids:
    placeholders = ",".join("?" for _ in event_ids)
    out["event_publications"] = rows(f"""
        WITH latest_metric AS (
            SELECT group_id, post_id, MAX(views) AS views, MAX(likes) AS likes,
                   MAX(comments) AS comments, MAX(reposts) AS reposts
            FROM vk_post_metric
            WHERE group_id=231920894
            GROUP BY group_id, post_id
        )
        SELECT ep.event_id, ep.target, ep.stored_url, ep.live_url,
               ep.stored_post_id, ep.live_post_id, ep.match_method,
               ep.match_confidence, ep.status, lm.views, lm.likes,
               lm.comments, lm.reposts
        FROM event_publication ep
        LEFT JOIN latest_metric lm ON lm.group_id=231920894 AND lm.post_id=ep.live_post_id
        WHERE ep.event_id IN ({placeholders}) AND ep.target='klgdevents'
        ORDER BY ep.event_id
    """, event_ids)
else:
    out["event_publications"] = []
print(json.dumps(out, ensure_ascii=False, default=str))
'''.replace("__TARGET_DATE__", json.dumps(target_date)).replace("__RUNS_LIMIT__", str(int(runs)))


def main() -> int:
    parser = argparse.ArgumentParser(description="Read-only future event inventory from events-bot production DB.")
    parser.add_argument("--date", required=True, help="Target event date, YYYY-MM-DD.")
    parser.add_argument("--app", default="events-bot-new-wngqia", help="Fly app name.")
    parser.add_argument("--runs", type=int, default=8, help="Recent poll runs for the date to include.")
    args = parser.parse_args()

    code_b64 = base64.b64encode(_remote_code(args.date, args.runs).encode()).decode()
    shell = textwrap.dedent(
        f"""
        set -a
        source /home/dev/.config/fly/release.env
        set +a
        export PATH="$HOME/.fly/bin:$PATH"
        flyctl ssh console -a {args.app!r} -C "python3 -c \\"import base64; exec(base64.b64decode('{code_b64}'))\\""
        """
    )
    proc = subprocess.run(["bash", "-lc", shell], text=True, capture_output=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr)
        sys.stdout.write(proc.stdout)
        return proc.returncode
    lines = [line for line in proc.stdout.splitlines() if not line.startswith("Connecting to ")]
    sys.stdout.write("\n".join(lines).strip() + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
