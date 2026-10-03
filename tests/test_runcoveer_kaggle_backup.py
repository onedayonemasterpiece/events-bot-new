"""Safety contracts for private production disaster-recovery uploads."""
import hashlib
import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


def setup_tool(tmp_path, monkeypatch, private=True):
    path = Path(__file__).resolve().parents[1] / 'deploy/runcoveer/kaggle-backup.py'
    spec = importlib.util.spec_from_file_location('backup_tool', path)
    tool = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(tool)
    tool.ROOT = tmp_path
    tool.UPLOAD = tmp_path / 'upload'
    tool.PENDING = tmp_path / 'pending.json'
    tool.UPLOAD.mkdir()
    payload = b'encrypted-fixture'
    digest = hashlib.sha256(payload).hexdigest()
    manifest = {'sha256': digest, 'bytes': len(payload), 'created_at_utc': 'fixture'}
    (tool.UPLOAD / 'backup.tar.gz.age').write_bytes(payload)
    (tool.UPLOAD / 'manifest.json').write_text(json.dumps(manifest))
    api = MagicMock()
    api.dataset_list.return_value = [SimpleNamespace(
        ref='test/runcoveer-production-backup', is_private=private, current_version_number=2,
    )]
    api.dataset_create_version.return_value = SimpleNamespace(error_message=None)
    api.dataset_status.return_value = 'ready'

    def download(ref, name, path, **kwargs):
        (Path(path) / name).write_text(json.dumps(manifest))

    api.dataset_download_file.side_effect = download
    sdk = MagicMock()
    sdk.KaggleApi.return_value = api
    monkeypatch.setitem(sys.modules, 'kaggle.api.kaggle_api_extended', sdk)
    monkeypatch.setenv('KAGGLE_USERNAME', 'test')
    monkeypatch.setattr(sys, 'argv', ['tool.py'])
    return tool, api, manifest


def test_public_dataset_refused_before_upload(tmp_path, monkeypatch):
    tool, api, _ = setup_tool(tmp_path, monkeypatch, private=False)
    with pytest.raises(RuntimeError, match='non-private'):
        tool.main()
    api.dataset_create_version.assert_not_called()
    api.dataset_create_new.assert_not_called()
    assert not tool.PENDING.exists()


def test_lost_response_reconciled_without_duplicate_upload(tmp_path, monkeypatch):
    tool, api, manifest = setup_tool(tmp_path, monkeypatch)
    tool.PENDING.write_text(json.dumps({
        'ref': 'test/runcoveer-production-backup', 'sha256': manifest['sha256'],
    }))
    tool.main()
    api.dataset_create_version.assert_not_called()
    api.dataset_create_new.assert_not_called()
    assert not tool.PENDING.exists()
    assert json.loads((tmp_path / 'receipt.json').read_text())['private'] is True


def test_different_pending_payload_never_republished(tmp_path, monkeypatch):
    tool, api, _ = setup_tool(tmp_path, monkeypatch)
    tool.PENDING.write_text(json.dumps({
        'ref': 'test/runcoveer-production-backup', 'sha256': 'different-backup',
    }))
    with pytest.raises(RuntimeError, match='Pending intent differs'):
        tool.main()
    api.dataset_create_version.assert_not_called()
    assert tool.PENDING.exists()


def test_update_preserves_history_and_verifies_privacy(tmp_path, monkeypatch):
    tool, api, _ = setup_tool(tmp_path, monkeypatch)
    tool.main()
    assert api.dataset_create_version.call_args.kwargs['delete_old_versions'] is False
    assert not tool.PENDING.exists()
