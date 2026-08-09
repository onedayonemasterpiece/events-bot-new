#!/usr/bin/env python3
"""Plan or apply the single persistent Unusual production-health issue."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path
from typing import Any, Mapping, Sequence

HEALTH_SCHEMA = "unusual-events-health-v1"
PLAN_SCHEMA = "unusual-events-health-issue-plan-v1"
MARKER = "<!-- unusual-events-production-health -->"
LABEL = "monitor:unusual-events"
REPOSITORY_RE = re.compile(r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
FINGERPRINT = "unusual-events-production-health:v1"


def _load(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError("health_not_object")
    return value


def build_issue_plan(health: Mapping[str, Any]) -> dict[str, Any]:
    if health.get("schema_version") != HEALTH_SCHEMA:
        raise ValueError("health_schema_invalid")
    status = str(health.get("health_status") or "")
    readiness = str(health.get("content_readiness") or "")
    if status not in {"HEALTHY", "WATCH", "INCIDENT"}:
        raise ValueError("health_status_invalid")
    if readiness not in {"READY", "NOT_READY", "BLOCKED"}:
        raise ValueError("content_readiness_invalid")
    closure = health.get("closure") if isinstance(health.get("closure"), Mapping) else {}
    if status in {"WATCH", "INCIDENT"}:
        action = "OPEN_OR_UPDATE"
    elif closure.get("eligible_to_close") is True:
        action = "CLOSE"
    else:
        action = "HOLD"
    source = health.get("source") if isinstance(health.get("source"), Mapping) else {}
    feed = health.get("feed") if isinstance(health.get("feed"), Mapping) else {}
    findings = health.get("findings") if isinstance(health.get("findings"), Mapping) else {}
    codes = [
        str(item.get("code"))[:120]
        for group in (findings.get("errors") or [], findings.get("warnings") or [])
        for item in ([group] if isinstance(group, Mapping) else [])
    ][:30]
    body_lines = [
        MARKER,
        "## Unusual events production health",
        "",
        f"- Health: **{status}**",
        f"- Content readiness: **{readiness}**",
        f"- Build: `{str(source.get('build_id') or 'unavailable')[:240]}`",
        f"- Run: `{str(source.get('run_id') or 'unavailable')[:240]}`",
        f"- Selected: **{int(feed.get('selected_count') or 0)}** / target {int(feed.get('target_count') or 0)} (minimum {int(feed.get('minimum_publish_count') or 0)})",
        f"- Closure streak: **{int(closure.get('consecutive_healthy_ready_runs') or 0)} / {int(closure.get('required_consecutive_runs') or 2)}**",
        "",
        "Finding codes: " + (", ".join(f"`{code}`" for code in codes) if codes else "none"),
        "",
        "Contract: `docs/features/unusual-events/unusual-events-production-health-v1.schema.json`",
    ]
    return {
        "schema_version": PLAN_SCHEMA,
        "fingerprint": FINGERPRINT,
        "action": action,
        "label": LABEL,
        "title": f"Unusual events health: {status} / {readiness}",
        "body": "\n".join(body_lines)[:12000],
        "close_comment": "Two consecutive distinct runs were HEALTHY and READY; the monitor is closing this issue.",
    }


def _gh(repository: str, *args: str, input_value: Mapping[str, Any] | None = None) -> Any:
    command = ["gh", "api", *args]
    completed = subprocess.run(
        command,
        input=(json.dumps(input_value) if input_value is not None else None),
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(f"gh_api_failed:{args[0] if args else 'unknown'}")
    if not completed.stdout.strip():
        return None
    return json.loads(completed.stdout)


def apply_issue_plan(plan: Mapping[str, Any], *, repository: str) -> dict[str, Any]:
    if plan.get("schema_version") != PLAN_SCHEMA:
        raise ValueError("plan_schema_invalid")
    if not REPOSITORY_RE.fullmatch(repository):
        raise ValueError("repository_invalid")
    action = str(plan.get("action") or "")
    if action not in {"OPEN_OR_UPDATE", "CLOSE", "HOLD"}:
        raise ValueError("plan_action_invalid")
    issues = _gh(repository, f"repos/{repository}/issues?state=all&labels={LABEL}&per_page=100")
    if not isinstance(issues, list):
        raise RuntimeError("issues_response_invalid")
    matches = [
        issue
        for issue in issues
        if isinstance(issue, Mapping) and not issue.get("pull_request") and MARKER in str(issue.get("body") or "")
    ]
    if len(matches) > 1:
        raise RuntimeError("duplicate_managed_issues")
    issue = matches[0] if matches else None
    if action == "HOLD":
        return {"action": "hold", "issue_number": int(issue["number"]) if issue else None}
    if action == "OPEN_OR_UPDATE":
        try:
            _gh(
                repository,
                f"repos/{repository}/labels",
                "--method",
                "POST",
                "--input",
                "-",
                input_value={"name": LABEL, "color": "B60205", "description": "Managed Unusual production health"},
            )
        except RuntimeError:
            # GitHub returns 422 when the label already exists.
            pass
        payload = {"title": plan["title"], "body": plan["body"], "labels": [LABEL]}
        if issue:
            number = int(issue["number"])
            _gh(repository, f"repos/{repository}/issues/{number}", "--method", "PATCH", "--input", "-", input_value={**payload, "state": "open"})
            return {"action": "update", "issue_number": number}
        created = _gh(repository, f"repos/{repository}/issues", "--method", "POST", "--input", "-", input_value=payload)
        return {"action": "create", "issue_number": int(created["number"])}
    if not issue or issue.get("state") != "open":
        return {"action": "close_noop", "issue_number": int(issue["number"]) if issue else None}
    number = int(issue["number"])
    _gh(repository, f"repos/{repository}/issues/{number}/comments", "--method", "POST", "--input", "-", input_value={"body": plan["close_comment"]})
    _gh(repository, f"repos/{repository}/issues/{number}", "--method", "PATCH", "--input", "-", input_value={"state": "closed", "state_reason": "completed"})
    return {"action": "close", "issue_number": number}


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--health", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--repository")
    args = parser.parse_args(argv)
    plan = build_issue_plan(_load(args.health))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(plan, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    if args.apply:
        result = apply_issue_plan(plan, repository=str(args.repository or ""))
        print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
