#!/usr/bin/env python3
"""Consistent SQLite backups; preserve non-SQLite files without copying secrets."""
import datetime, json, os, sqlite3, subprocess, shutil
from pathlib import Path
os.umask(0o077)
root = Path('/var/lib/runcoveer')
backup = Path('/var/backups/runcoveer') / datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')
# Reserve room for the new snapshot and worst-case ciphertext, retaining 2 GiB.
source_bytes = sum(p.stat().st_size for p in root.rglob('*')
                   if p.is_file() and not p.is_symlink())
required_bytes = 2 * source_bytes + 2 * 1024 ** 3
free_bytes = shutil.disk_usage(root).free
if free_bytes < required_bytes:
    raise RuntimeError(f'Backup deferred: free_bytes={free_bytes} required_bytes={required_bytes}')
backup.mkdir(parents=True)
try:
    receipts = []
    for service in ('events', 'kotopogoda'):
        target = backup / service
        target.mkdir()
        for db in (root / service).rglob('*'):
            if not db.is_file() or db.suffix not in ('.db', '.sqlite', '.sqlite3'):
                continue
            with db.open('rb') as f:
                if f.read(16) != b'SQLite format 3\x00':
                    continue
            dest = target / db.relative_to(root / service)
            dest.parent.mkdir(parents=True, exist_ok=True)
            src = sqlite3.connect(f'file:{db}?mode=ro', uri=True, timeout=30)
            out = sqlite3.connect(dest)
            src.backup(out)
            assert out.execute('pragma quick_check').fetchone()[0] == 'ok'
            out.close(); src.close()
            receipts.append(str(dest.relative_to(backup)))
        subprocess.run(['rsync', '-a', '--exclude=*.db*', '--exclude=*.sqlite*', '--exclude=runtime_logs/', str(root/service)+'/', str(target)+'/'], check=True)
    (backup/'receipt.json').write_text(json.dumps({'sqlite': receipts}, indent=2))
except BaseException:
    # Only this invocation's incomplete snapshot; completed recovery stays intact.
    shutil.rmtree(backup)
    raise
# Only prune backups made by this script after a successful complete backup.
managed = []
for old in backup.parent.iterdir():
    receipt = old / 'receipt.json'
    if old.is_dir() and receipt.is_file() and not old.is_symlink():
        try:
            data = json.loads(receipt.read_text())
            if isinstance(data.get('sqlite'), list) and len(old.name) == 16:
                datetime.datetime.strptime(old.name, '%Y%m%dT%H%M%SZ')
                managed.append(old)
        except (ValueError, OSError):
            pass
for old in sorted(managed, reverse=True)[1:]:
    shutil.rmtree(old)
print(backup)
