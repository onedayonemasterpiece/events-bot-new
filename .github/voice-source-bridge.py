"""Temporary, pinned source transfer for PR587. Immutable blobs only; no ref writes."""
import base64
import hashlib
import json
import lzma
import os
from pathlib import Path, PurePosixPath
import subprocess
import tempfile
import urllib.request

REPO = 'onedayonemasterpiece/events-bot-new'
BASE = '066e5bccba1f8bc7037e140bb8c4c461111c5272'
PARTS = ['17a06356ba958ae04572c6920a13d596e22d8ce7',
         '10110d926922262ccf1b83f84e065dfb7634fe39',
         'a15f515118e76f4e40d68684c2636f12e3fdb7ab',
         '28b555d05095f69c96be288dd594498ce53bac2e']
DIGEST = '0d54955f8e58b6760f35ac76ccd76bdc1a159bc859f0ada93f795896f83b9dc4'

def git(*args, cwd=None, data=None, check=True):
    return subprocess.run(['git', *args], cwd=cwd, input=data, check=check,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)

def blob_hash(data):
    return hashlib.sha1(b'blob ' + str(len(data)).encode() + b'\0' + data).hexdigest()

def api(suffix, data=None):
    req = urllib.request.Request('https://api.github.com/repos/' + REPO + suffix,
        data=json.dumps(data).encode() if data is not None else None,
        headers={'Authorization': 'Bearer ' + os.environ['GH_TOKEN'],
                 'Accept': 'application/vnd.github+json', 'Content-Type': 'application/json'})
    with urllib.request.urlopen(req, timeout=45) as response:
        return json.loads(response.read(4_000_000))

def main():
    event = json.loads(Path(os.environ['GITHUB_EVENT_PATH']).read_text())
    pr = event['pull_request']
    assert event['number'] == 587
    assert pr['head']['repo']['full_name'] == REPO
    assert pr['head']['ref'] == 'docs/agent-assisted-event-discovery-20260826'
    archive = b''
    for sha in PARTS:
        response = api('/git/blobs/' + sha)
        raw = base64.b64decode(response['content'])
        assert blob_hash(raw) == sha
        archive += raw
    assert hashlib.sha256(archive).hexdigest() == DIGEST
    dec = lzma.LZMADecompressor(memlimit=128 * 1024 * 1024)
    raw = dec.decompress(archive, max_length=1_000_001)
    assert dec.eof and not dec.unused_data and len(raw) <= 1_000_000
    package = json.loads(raw)
    assert package['base'] == BASE and len(package['files']) == 38
    allowed_prefixes = ('site/', 'supabase/', 'infra/yandex/supabase-relay/')
    exact = {'CHANGELOG.md', 'scripts/ops/voice_pwa_device_probe.py',
             '.github/workflows/postbox-sql-contract.yml'}
    files = package['files']
    paths = {f['path'] for f in files}
    assert len(paths) == len(files)
    git('cat-file', '-e', BASE + '^{commit}')
    with tempfile.TemporaryDirectory(prefix='voice-pinned-source-') as work:
        for f in files:
            path = f['path']
            assert not PurePosixPath(path).is_absolute()
            assert '..' not in PurePosixPath(path).parts and '.git' not in PurePosixPath(path).parts
            assert path in exact or path.startswith(allowed_prefixes)
            target = Path(work) / path
            before = git('show', BASE + ':' + path, check=False)
            if f['before'] is None:
                assert before.returncode != 0
            else:
                assert before.returncode == 0 and blob_hash(before.stdout) == f['before']
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(before.stdout)
        git('init', '-q', cwd=work)
        git('add', '.', cwd=work)
        patch = package['patch'].encode()
        touched = git('apply', '--numstat', cwd=work, data=patch).stdout.decode().splitlines()
        assert {line.split('\t', 2)[2] for line in touched} == paths
        git('apply', '--check', cwd=work, data=patch)
        git('apply', cwd=work, data=patch)
        result = []
        for f in files:
            target = Path(work) / f['path']
            assert not target.is_symlink() and target.is_file()
            data = target.read_bytes()
            assert len(data) <= 2_000_000 and blob_hash(data) == f['after']
            created = api('/git/blobs', {'encoding': 'base64',
                'content': base64.b64encode(data).decode()})
            assert created['sha'] == f['after']
            result.append({'path': f['path'], 'mode': '100644', 'type': 'blob', 'sha': created['sha']})
    output = {'base': BASE, 'compressed_sha256': DIGEST, 'files': result,
              'refs_written': False, 'source_executed': False}
    Path('/tmp/voice-source-blobs.json').write_text(json.dumps(output, indent=2) + '\n')
    print('Verified and stored', len(result), 'immutable source blobs; no ref changes.')

if __name__ == '__main__':
    main()
