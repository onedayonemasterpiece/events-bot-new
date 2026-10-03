#!/usr/bin/env python3
"""Bounded host maintenance; never remove service data, backups or images."""
import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess

GIB = 1024 ** 3


def disk_state(path='/'):
    usage = shutil.disk_usage(path)
    stat = os.statvfs(path)
    used_percent = 100 * (usage.total - usage.free) / usage.total
    inode_percent = 100 * (stat.f_files - stat.f_favail) / max(1, stat.f_files)
    severity = 'ok'
    if usage.free < 2 * GIB or used_percent >= 80 or inode_percent >= 80:
        severity = 'warning'
    if usage.free < GIB or used_percent >= 90 or inode_percent >= 90:
        severity = 'critical'
    return dict(severity=severity, free_bytes=usage.free,
                used_percent=round(used_percent, 2),
                inode_used_percent=round(inode_percent, 2))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--apply', action='store_true')
    args = parser.parse_args()
    before = disk_state()
    commands = [['docker', 'builder', 'prune', '-f', '--filter', 'until=168h']]
    if before['severity'] != 'ok':
        commands.append(['apt-get', 'clean'])
    actions = []
    if args.apply:
        for command in commands:
            result = subprocess.run(command, capture_output=True, timeout=120)
            # Do not copy arbitrary daemon output into the status receipt.
            actions.append(dict(command=command, returncode=result.returncode))
    after = disk_state()
    report = dict(before=before, after=after, apply=args.apply,
                  planned_commands=commands, actions=actions)
    if args.apply:
        root = Path('/run/runcoveer')
        root.mkdir(mode=0o700, exist_ok=True)
        temporary = root / 'disk-status.json.partial'
        temporary.write_text(json.dumps(report))
        temporary.replace(root / 'disk-status.json')
    print(json.dumps(report), flush=True)
    return 1 if after['severity'] == 'critical' or any(
        action['returncode'] for action in actions) else 0


if __name__ == '__main__':
    raise SystemExit(main())
