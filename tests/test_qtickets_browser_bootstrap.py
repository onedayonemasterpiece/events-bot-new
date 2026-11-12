"""Exercise the actual startup boundary without downloading browsers in unit tests."""
import ast
import builtins
from pathlib import Path
import subprocess
from types import SimpleNamespace

import pytest


def bootstrap(*, installed=True, fail_runtime=False):
    source = Path('kaggle/ParseQtickets/parse_qtickets.py').read_text()
    node = next(n for n in ast.parse(source).body if isinstance(n, ast.FunctionDef) and n.name == 'ensure_playwright')
    calls = []
    state = {'installed': installed}
    def importing(name, *args, **kwargs):
        if name == 'playwright.sync_api':
            if not state['installed']:
                raise ImportError(name)
            return SimpleNamespace(sync_playwright=object())
        return builtins.__import__(name, *args, **kwargs)
    def check_call(command, **kwargs):
        calls.append(command)
        if 'pip' in command:
            state['installed'] = True
        if '--with-deps' in command and fail_runtime:
            raise subprocess.CalledProcessError(100, command)
    ns = {'sys': SimpleNamespace(executable='python'), 'subprocess': SimpleNamespace(check_call=check_call, run=lambda *a, **k: SimpleNamespace(returncode=100)), '__builtins__': {**vars(builtins), '__import__': importing}}
    exec(compile(ast.Module(body=[node], type_ignores=[]), '<actual-qtickets-bootstrap>', 'exec'), ns)
    return ns['ensure_playwright'], calls


def test_existing_python_package_does_not_mean_browser_is_ready():
    run, calls = bootstrap(installed=True)
    assert run() is True
    assert calls == [['python', '-m', 'playwright', 'install', '--with-deps', 'chromium']]


def test_missing_package_is_installed_before_browser_runtime():
    run, calls = bootstrap(installed=False)
    assert run() is True
    assert calls[0] == ['python', '-m', 'pip', 'install', 'playwright', '-q']
    assert calls[1] == ['python', '-m', 'playwright', 'install', '--with-deps', 'chromium']


def test_system_dependency_failure_cannot_report_browser_ready():
    run, _ = bootstrap(fail_runtime=True)
    with pytest.raises(subprocess.CalledProcessError):
        run()
