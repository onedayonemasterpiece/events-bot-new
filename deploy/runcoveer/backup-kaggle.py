#!/usr/bin/env python3
"""Snapshot, encrypt with a public age recipient, then publish off-host."""
import datetime
import fcntl
import hashlib
import json
import os
import subprocess
from pathlib import Path

os.umask(0o077)
DEPLOY = Path('/opt/runcoveer/deployment')
ROOT = Path('/var/backups/runcoveer/kaggle')
ROOT.mkdir(parents=True, exist_ok=True)
(ROOT / 'sdk-tmp').mkdir(exist_ok=True)
with (ROOT / 'lock').open('a') as lock:
    fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
    upload = ROOT / 'upload'
    upload.mkdir(exist_ok=True)
    if not (ROOT / 'pending.json').exists():
        result = subprocess.run(['python3', str(DEPLOY / 'backup.py')],
                                check=True, text=True, capture_output=True)
        snapshot = Path(result.stdout.strip())
        archive = upload / 'backup.tar.gz.age'
        temporary = upload / 'backup.tar.gz.age.partial'
        tar = subprocess.Popen([
            'tar', '-czf', '-', '-C', str(snapshot), '.',
            '-C', '/opt/runcoveer', 'events/.env.production',
            'kotopogoda/.env.production', 'deployment',
            '-C', '/etc', 'caddy/Caddyfile', 'runcoveer/backup-age.recipient',
        ], stdout=subprocess.PIPE)
        try:
            with temporary.open('wb') as output:
                encryption = subprocess.run([
                    'age', '-R', '/etc/runcoveer/backup-age.recipient',
                ], stdin=tar.stdout, stdout=output)
            tar.stdout.close()
            if tar.wait() or encryption.returncode:
                raise RuntimeError('Backup encryption failed')
            temporary.replace(archive)
        finally:
            if tar.poll() is None:
                tar.kill()
                tar.wait()
        with archive.open('rb') as stream:
            digest = hashlib.file_digest(stream, 'sha256').hexdigest()
        (upload / 'manifest.json').write_text(json.dumps({
            'schema': 1, 'encryption': 'age', 'sha256': digest,
            'bytes': archive.stat().st_size,
            'created_at_utc': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        }))
    subprocess.run([
        'docker', 'run', '--rm', '--env-file', '/opt/runcoveer/events/.env.production',
        '--mount', f'type=bind,src={ROOT},dst=/backup',
        '--mount', f'type=bind,src={DEPLOY / "kaggle-backup.py"},dst=/tool.py,readonly',
        '--env', 'TMPDIR=/backup/sdk-tmp', '--entrypoint', 'python',
        'runcoveer/events:8105d069f', '/tool.py',
    ], check=True)
