#!/usr/bin/env python3
"""Private encrypted Kaggle backup; one dataset and reusable filenames. Run in the Events image."""
import contextlib
import hashlib
import json
import os
import sys
import time
import zipfile
import shutil
from pathlib import Path


ROOT = Path('/backup')
UPLOAD = ROOT / 'upload'
PENDING = ROOT / 'pending.json'


def sha(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def main():
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()
    owner = os.environ['KAGGLE_USERNAME']
    ref = owner + '/runcoveer-production-backup'
    datasets = api.dataset_list(mine=True, search='runcoveer-production-backup') or []
    existing = next((d for d in datasets if d.ref == ref), None)
    if existing is not None and existing.is_private is not True:
        raise RuntimeError('Refusing to upload to a non-private dataset')
    manifest = json.loads((UPLOAD / 'manifest.json').read_text())
    if sha(UPLOAD / 'backup.tar.gz.age') != manifest['sha256']:
        raise RuntimeError('Encrypted archive checksum mismatch')
    if PENDING.exists():
        intent = json.loads(PENDING.read_text())
        if intent['ref'] != ref or intent['sha256'] != manifest['sha256']:
            raise RuntimeError('Pending intent differs from staged backup')
    if '--download' not in sys.argv and not PENDING.exists():
        (UPLOAD / 'dataset-metadata.json').write_text(json.dumps({
            'id': ref, 'title': 'RunCoveer encrypted production backup',
            'description': 'Encrypted disaster recovery backup.',
            'licenses': [{'name': 'other'}], 'isPrivate': True,
        }))
        PENDING.write_text(json.dumps({'ref': ref, 'sha256': manifest['sha256']}))
        # Persist intent before the API mutation. Lost responses are reconciled;
        # an unknown upload is never repeated blindly on the next timer run.
        if existing is None:
            response = api.dataset_create_new(str(UPLOAD), public=False,
                                             quiet=True, convert_to_csv=False)
        else:
            response = api.dataset_create_version(
                str(UPLOAD), version_notes='Encrypted latest backup ' + manifest['sha256'],
                quiet=True, convert_to_csv=False, delete_old_versions=False,
            )
        if getattr(response, 'error_message', None):
            raise RuntimeError('Kaggle rejected the dataset mutation; intent retained')
    readback = ROOT / 'readback'
    readback.mkdir(exist_ok=True)

    def download(name):
        with open(os.devnull, 'w') as sink, contextlib.redirect_stdout(sink):
            api.dataset_download_file(ref, name, path=str(readback), force=True)
        target = readback / name
        wrapped = readback / (name + '.zip')
        if wrapped.exists():
            with zipfile.ZipFile(wrapped) as bundle:
                if bundle.namelist() != [name]:
                    raise RuntimeError('Unexpected download archive members')
                with bundle.open(name) as source, target.open('wb') as output:
                    shutil.copyfileobj(source, output)
            wrapped.unlink()
        return target

    for _ in range(60):
        try:
            status = api.dataset_status(ref)
            if str(status).lower() == 'ready':
                # Do not expose signed download URLs or SDK chatter in logs.
                downloaded = download('manifest.json')
                if json.loads(downloaded.read_text()).get('sha256') == manifest['sha256']:
                    break
        except Exception:
            pass
        time.sleep(10)
    else:
        raise RuntimeError('Upload not reconciled; pending intent retained, no blind retry')
    datasets = api.dataset_list(mine=True, search='runcoveer-production-backup') or []
    current = next((d for d in datasets if d.ref == ref), None)
    if current is None or current.is_private is not True:
        raise RuntimeError('Dataset privacy could not be verified')
    if '--download' in sys.argv:
        downloaded = download('backup.tar.gz.age')
        if sha(downloaded) != manifest['sha256']:
            raise RuntimeError('Downloaded archive checksum mismatch')
    receipt = {'ref': ref, 'private': True, 'version': current.current_version_number,
               'sha256': manifest['sha256'], 'bytes': manifest['bytes'],
               'created_at_utc': manifest['created_at_utc'], 'old_versions_deleted': False}
    (ROOT / 'receipt.json').write_text(json.dumps(receipt, indent=2))
    PENDING.unlink(missing_ok=True)
    print(json.dumps(receipt))


if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        print(json.dumps({'component': 'kaggle_backup', 'status': 'failed',
                          'error_type': type(exc).__name__, 'pending': PENDING.exists()}))
        sys.exit(1)
