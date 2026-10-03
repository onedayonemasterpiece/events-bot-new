"""Disk guard must signal pressure and never prune production state."""
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace


def load_guard():
    path = Path(__file__).parents[1] / 'deploy/runcoveer/disk-maintenance.py'
    spec = importlib.util.spec_from_file_location('capacity_guard', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_inode_exhaustion_is_critical_even_with_free_bytes(monkeypatch):
    guard = load_guard()
    monkeypatch.setattr(guard.shutil, 'disk_usage', lambda _: SimpleNamespace(
        total=25 * guard.GIB, free=10 * guard.GIB))
    monkeypatch.setattr(guard.os, 'statvfs', lambda _: SimpleNamespace(
        f_files=1000, f_favail=50))
    assert guard.disk_state()['severity'] == 'critical'


def test_dry_run_does_not_execute_cleanup(monkeypatch, capsys):
    guard = load_guard()
    monkeypatch.setattr(sys, 'argv', ['disk-maintenance.py'])
    monkeypatch.setattr(guard, 'disk_state', lambda: {'severity': 'warning'})
    def forbidden(*args, **kwargs):
        raise AssertionError('dry run must not execute commands')
    monkeypatch.setattr(guard.subprocess, 'run', forbidden)
    assert guard.main() == 0
    assert 'apt-get' in capsys.readouterr().out


def test_critical_cleanup_preserves_images_volumes_and_backups(monkeypatch, tmp_path):
    guard = load_guard()
    monkeypatch.setattr(sys, 'argv', ['disk-maintenance.py', '--apply'])
    monkeypatch.setattr(guard, 'disk_state', lambda: {'severity': 'critical'})
    monkeypatch.setattr(guard, 'Path', lambda _: tmp_path)
    commands = []
    def run(command, **kwargs):
        commands.append(command)
        return SimpleNamespace(returncode=0)
    monkeypatch.setattr(guard.subprocess, 'run', run)
    assert guard.main() == 1
    assert commands == [
        ['docker', 'builder', 'prune', '-f', '--filter', 'until=168h'],
        ['apt-get', 'clean'],
    ]
    assert (tmp_path / 'disk-status.json').exists()
