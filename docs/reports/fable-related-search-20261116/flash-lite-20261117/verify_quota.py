"""Summarize the frozen, anonymized normal-pool quota report without live access."""
import json
import statistics
import sys
from pathlib import Path

path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(__file__).with_name("quota-evidence.json")
data = json.loads(path.read_text())
days = sorted(data["daily_used_per_scope"])
assert days[-1] == data["window_open"]
for limits in data["limits_per_project_model"]:
    model = limits["model"]
    ceiling = data["normal_active_scopes"] * limits["rpd"]
    usage = [sum(data["daily_used_per_scope"][day][model].values()) for day in days]
    remaining = {scope: limits["rpd"] - amount
                 for scope, amount in data["daily_used_per_scope"][days[-1]][model].items()}
    previous, last24 = usage[:-1], data["physical_attempts_by_model"][model]
    assert all(n >= 0 for n in remaining.values())
    assert sum(last24["statuses"].values()) == last24["total"] == sum(last24["consumers"].values())
    assert sum(remaining.values()) == ceiling - usage[-1]
    print(model, "RPD used/ceiling", f"{usage[-1]}/{ceiling}", "remaining", sum(remaining.values()),
          "previous complete days (min, median, max)", (min(previous), statistics.median(previous), max(previous)),
          "last 24h", last24["statuses"])
