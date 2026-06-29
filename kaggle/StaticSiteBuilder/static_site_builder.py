from __future__ import annotations

import base64
import json
import os
import shutil
import subprocess
import tarfile
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent
WORKING = Path('/kaggle/working') if Path('/kaggle/working').exists() else ROOT / 'output'
INPUT_ROOT = Path('/kaggle/input') if Path('/kaggle/input').exists() else ROOT / 'input'
EXTRACT_ROOT = Path('/tmp/kenigevents-static-site') if Path('/kaggle').exists() else WORKING
SITE_DIR = EXTRACT_ROOT / 'site'
CONFIG_PATH = ROOT / 'build_config.json'
RESULT_PATH = WORKING / 'static_site_build_result.json'
EMBEDDED_SITE_SOURCE_B64 = ''
EMBEDDED_BUILD_CONFIG_JSON = ''

try:
    from kaggle_status_client import load_status_client
except Exception:  # pragma: no cover - local non-Kaggle fallback
    load_status_client = None

STATUS_CLIENT = None
STATUS_PROGRESS: dict[str, object] = {
    'phase': 'bootstrap',
    'progress_percent': 0,
    'progress_label': 'подготовка',
}
ACQUIRED_RESOURCES: list[str] = []


def status_update(**items: object) -> dict[str, object]:
    STATUS_PROGRESS.update({k: v for k, v in items.items() if v is not None})
    return dict(STATUS_PROGRESS)


def status_event(event: str, *, phase: str | None = None, status: str | None = None, progress: dict | None = None, message: str | None = None) -> None:
    if STATUS_CLIENT is None or not getattr(STATUS_CLIENT, 'enabled', False):
        return
    merged = status_update(**(progress or {}))
    STATUS_CLIENT.event(
        event,
        phase=phase or str(merged.get('phase') or event),
        status=status or 'running',
        progress=merged,
        message=message,
    )


def init_status() -> None:
    global STATUS_CLIENT
    if load_status_client is None:
        return
    STATUS_CLIENT = load_status_client(output_dir=WORKING, log=lambda message: print(message, flush=True))
    if not getattr(STATUS_CLIENT, 'enabled', False):
        return
    status_event('kernel_started', phase='preflight', status='running', progress={'phase': 'preflight', 'progress_percent': 2, 'progress_label': 'запуск Kaggle'})
    for resource_key in STATUS_CLIENT.config.get('resource_leases') or []:
        if not STATUS_CLIENT.acquire_resource(str(resource_key), ttl_seconds=3 * 60 * 60):
            raise RuntimeError(f'Required Kaggle resource is busy: {resource_key}')
        ACQUIRED_RESOURCES.append(str(resource_key))
    STATUS_CLIENT.start_alive(interval_seconds=60, progress_provider=lambda: dict(STATUS_PROGRESS))


def finish_status(*, ok: bool, message: str | None = None) -> None:
    try:
        if ok:
            status_event('report_written', phase='report', status='done', progress={'phase': 'report', 'progress_percent': 100, 'progress_label': 'готово'}, message=message)
        else:
            status_event('report_written', phase='failed', status='failed', progress={'phase': 'failed', 'progress_label': 'ошибка'}, message=message)
    finally:
        if STATUS_CLIENT is not None:
            for resource_key in list(ACQUIRED_RESOURCES):
                try:
                    STATUS_CLIENT.release_resource(resource_key)
                    ACQUIRED_RESOURCES.remove(resource_key)
                except Exception as exc:
                    print(f'[kaggle_status] resource release failed: {exc}', flush=True)
            STATUS_CLIENT.stop_alive()


def run(cmd: list[str], cwd: Path, env: dict[str, str] | None = None) -> None:
    print(f"[static-site-builder] $ {' '.join(cmd)} cwd={cwd}", flush=True)
    subprocess.run(cmd, cwd=str(cwd), env=env, check=True)


def find_input_file(name: str) -> Path | None:
    if not INPUT_ROOT.exists():
        return None
    for path in INPUT_ROOT.rglob(name):
        if path.is_file():
            return path
    return None


def read_config() -> dict:
    if EMBEDDED_BUILD_CONFIG_JSON.strip():
        return json.loads(EMBEDDED_BUILD_CONFIG_JSON)
    input_config = find_input_file('build_config.json')
    for path in [CONFIG_PATH, input_config]:
        if path and path.exists():
            return json.loads(path.read_text(encoding='utf-8'))
    return {}


def ensure_site_source() -> None:
    if SITE_DIR.exists():
        return
    # Use a non-archive-looking extension in the Kaggle dataset. Kaggle extracts
    # `.tar.gz` payloads while ingesting datasets; that breaks Astro dynamic route
    # filenames like `[slug].astro` and also hides the original archive from the
    # mounted input. The content is still a gzip tarball.
    archive = find_input_file('site_source.tarball') or find_input_file('site_source.tar.gz')
    if not archive and EMBEDDED_SITE_SOURCE_B64.strip():
        archive = WORKING / 'embedded_site_source.tarball'
        WORKING.mkdir(parents=True, exist_ok=True)
        archive.write_bytes(base64.b64decode(EMBEDDED_SITE_SOURCE_B64.encode('ascii')))
    if not archive:
        raise FileNotFoundError(f'site source not staged and site_source.tar.gz not found under {INPUT_ROOT}')
    print(f'[static-site-builder] extracting {archive} -> {EXTRACT_ROOT}', flush=True)
    EXTRACT_ROOT.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive, 'r:gz') as tar:
        tar.extractall(EXTRACT_ROOT)


def load_kaggle_secret_to_env(secret_name: str | None) -> None:
    name = (secret_name or '').strip()
    if not name or os.environ.get(name):
        return
    try:
        from kaggle_secrets import UserSecretsClient  # type: ignore

        value = (UserSecretsClient().get_secret(name) or '').strip()
        if value:
            os.environ[name] = value
            print(f'[static-site-builder] loaded Kaggle secret env={name}', flush=True)
    except Exception as exc:
        print(f'[static-site-builder] Kaggle secret {name} unavailable: {exc}', flush=True)


def load_encrypted_secrets_to_env() -> None:
    enc_path = find_input_file('secrets.enc')
    key_path = find_input_file('fernet.key') or find_input_file('fernet.keys')
    if not enc_path or not key_path:
        print('[static-site-builder] encrypted secret datasets unavailable', flush=True)
        return
    try:
        from cryptography.fernet import Fernet, MultiFernet

        if key_path.name == 'fernet.keys':
            keys = [line.strip().encode('ascii') for line in key_path.read_text(encoding='utf-8').splitlines() if line.strip()]
            fernet = MultiFernet([Fernet(key) for key in keys])
        else:
            fernet = Fernet(key_path.read_bytes().strip())
        payload = json.loads(fernet.decrypt(enc_path.read_bytes()).decode('utf-8'))
        loaded: list[str] = []
        for name, value in (payload or {}).items():
            clean_name = str(name or '').strip()
            clean_value = str(value or '').strip()
            if not clean_name or not clean_value or os.environ.get(clean_name):
                continue
            os.environ[clean_name] = clean_value
            loaded.append(clean_name)
        print(f'[static-site-builder] loaded encrypted secret envs={sorted(loaded)}', flush=True)
    except Exception as exc:
        print(f'[static-site-builder] encrypted secrets unavailable: {exc}', flush=True)


def ensure_python_deps_for_gemma(config: dict) -> None:
    if not (config.get('gemma_related_verify') or config.get('related_mode') == 'pgvector' or config.get('sync_pgvector_vectors')):
        return
    status_event('alive', phase='export', status='alive', progress={'phase': 'export', 'progress_percent': 16, 'progress_label': 'python deps для Gemma limiter'})
    packages = ['cryptography>=42.0.0']
    if config.get('gemma_related_verify'):
        packages.extend(['supabase==2.16.0', 'google-genai>=1.75.0'])
    run(['python3', '-m', 'pip', 'install', '--quiet', *packages], cwd=WORKING, env=os.environ.copy())


def export_preview_data_if_configured(config: dict) -> None:
    if not config.get('export_in_kaggle'):
        return
    db_filename = (config.get('sqlite_db_filename') or '').strip()
    db_path = find_input_file(db_filename) if db_filename else None
    if not db_path:
        raise FileNotFoundError(f'Kaggle export requested but sqlite DB not found: {db_filename}')
    working_db_path = WORKING / db_filename
    if not working_db_path.exists():
        shutil.copy2(db_path, working_db_path)
    db_path = working_db_path
    exporter = SITE_DIR / 'scripts' / 'export-production-preview-data.py'
    if not exporter.exists():
        raise FileNotFoundError(f'preview exporter missing: {exporter}')
    cache_filename = (config.get('related_cache_filename') or 'event_related_chain_cache.json').strip()
    input_cache = find_input_file(cache_filename)
    cache_path = WORKING / cache_filename
    if input_cache and not cache_path.exists():
        shutil.copy2(input_cache, cache_path)
    key_env = (config.get('gemma_related_key_env') or 'GOOGLE_API_KEY4').strip()
    if config.get('gemma_related_verify') or config.get('related_mode') == 'pgvector' or config.get('sync_pgvector_vectors'):
        ensure_python_deps_for_gemma(config)
        load_encrypted_secrets_to_env()
    if config.get('gemma_related_verify'):
        load_kaggle_secret_to_env(key_env)
        for secret_name in ['SUPABASE_URL', 'SUPABASE_KEY', 'SUPABASE_SERVICE_KEY', 'SUPABASE_SERVICE_ROLE_KEY']:
            load_kaggle_secret_to_env(secret_name)
    cmd = [
        'python3',
        str(exporter),
        '--db', str(db_path),
        '--limit', str(config.get('limit') or 50),
        '--current-date', str(config.get('current_date') or os.environ.get('STATIC_SITE_CURRENT_DATE') or '2026-06-28'),
        '--output-dir', str(SITE_DIR / 'src/data'),
        '--related-cache', str(cache_path),
        '--related-mode', str(config.get('related_mode') or 'sparse'),
        '--pgvector-embedding-model', str(config.get('pgvector_embedding_model') or 'gemini-embedding-2'),
        '--pgvector-embedding-key-env', str(config.get('pgvector_embedding_key_env') or 'GOOGLE_API_KEY4'),
        '--pgvector-max-provider-calls', str(config.get('pgvector_max_provider_calls') or 1000),
        '--site-origin', str(config.get('public_site_origin') or 'https://kenigevents.ru'),
        '--base-path', str(config.get('build_id') or ''),
        '--ics-base-url', str(config.get('ics_base_url') or ''),
        '--gemma-related-model', str(config.get('gemma_related_model') or 'models/gemma-4-26b-a4b-it'),
        '--gemma-related-key-env', key_env,
        '--gemma-related-max-anchors', str(config.get('gemma_related_max_anchors') or 0),
    ]
    if config.get('sync_pgvector_vectors'):
        cmd.append('--sync-pgvector-vectors')
    if config.get('gemma_related_verify'):
        cmd.append('--gemma-related-verify')
    status_event('alive', phase='export', status='alive', progress={'phase': 'export', 'progress_percent': 18, 'progress_label': 'экспорт событий и related v2'})
    run(cmd, cwd=SITE_DIR, env=os.environ.copy())


def ensure_node22(env: dict[str, str]) -> dict[str, str]:
    current = subprocess.run(['node', '--version'], cwd=str(WORKING), env=env, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    version = (current.stdout or '').strip()
    major = 0
    if version.startswith('v'):
        try:
            major = int(version[1:].split('.', 1)[0])
        except Exception:
            major = 0
    if major >= 22:
        return env
    status_event('alive', phase='preflight', status='alive', progress={'phase': 'preflight', 'progress_percent': 8, 'progress_label': f'установка Node 22 вместо {version or "unknown"}'})
    node_prefix = WORKING / 'node22'
    run(['npm', 'install', '--prefix', str(node_prefix), '--no-audit', '--no-fund', 'node@22.12.0'], cwd=WORKING, env=env)
    node_bin = node_prefix / 'node_modules' / 'node' / 'bin'
    if not (node_bin / 'node').exists():
        raise FileNotFoundError(f'Node 22 binary missing: {node_bin / "node"}')
    new_env = dict(env)
    new_env['PATH'] = str(node_bin) + os.pathsep + new_env.get('PATH', '')
    return new_env


def main() -> int:
    init_status()
    started = datetime.now(timezone.utc).isoformat()
    try:
        config = read_config()
        build_id = config.get('build_id') or os.environ.get('PREVIEW_BUILD_ID') or 'preview-kaggle-static-site'
        status_event('alive', phase='preflight', status='alive', progress={'phase': 'preflight', 'progress_percent': 5, 'progress_label': 'распаковка сайта', 'build_id': build_id})
        ensure_site_source()
        if not SITE_DIR.exists():
            raise FileNotFoundError(f"site source not staged: {SITE_DIR}")
        WORKING.mkdir(parents=True, exist_ok=True)
        env = os.environ.copy()
        env['PREVIEW_BUILD_ID'] = build_id
        env['PUBLIC_SITE_ORIGIN'] = config.get('public_site_origin') or env.get('PUBLIC_SITE_ORIGIN') or 'https://kenigevents.ru'
        env['SITE_BASE_PATH'] = f'/{build_id}'
        env['PUBLIC_PREVIEW_BUILD_ID'] = build_id
        if config.get('asset_base_url'):
            env['PUBLIC_ASSET_BASE_URL'] = str(config['asset_base_url'])
        if config.get('astro_asset_base_url'):
            env['PUBLIC_ASTRO_ASSET_BASE_URL'] = str(config['astro_asset_base_url'])
        if config.get('ics_base_url'):
            env['PUBLIC_ICS_BASE_URL'] = str(config['ics_base_url'])
        export_preview_data_if_configured(config)
        env = ensure_node22(env)

        status_event('preflight_ok', phase='preflight', status='done', progress={'phase': 'preflight', 'progress_percent': 15, 'progress_label': 'окружение готово'})
        run(['node', '--version'], cwd=SITE_DIR, env=env)
        run(['npm', '--version'], cwd=SITE_DIR, env=env)
        status_event('alive', phase='install', status='alive', progress={'phase': 'install', 'progress_percent': 25, 'progress_label': 'установка npm зависимостей'})
        install_cmd = ['npm', 'ci', '--no-audit', '--no-fund'] if (SITE_DIR / 'package-lock.json').exists() else ['npm', 'install', '--no-audit', '--no-fund']
        run(install_cmd, cwd=SITE_DIR, env=env)
        status_event('alive', phase='build', status='alive', progress={'phase': 'build', 'progress_percent': 45, 'progress_label': 'Astro build'})
        run(['npm', 'run', 'build:preview'], cwd=SITE_DIR, env=env)
        status_event('alive', phase='check', status='alive', progress={'phase': 'check', 'progress_percent': 75, 'progress_label': 'проверка preview'})
        run(['npm', 'run', 'check:preview'], cwd=SITE_DIR, env=env)

        dist_dir = SITE_DIR / 'dist' / build_id
        if not dist_dir.exists():
            raise FileNotFoundError(f"build output missing: {dist_dir}")
        status_event('alive', phase='archive', status='alive', progress={'phase': 'archive', 'progress_percent': 90, 'progress_label': 'архивация результата'})
        archive_path = WORKING / f'{build_id}.tar.gz'
        with tarfile.open(archive_path, 'w:gz') as tar:
            tar.add(dist_dir, arcname=build_id)
        event_count = len(json.loads((SITE_DIR / 'src/data/preview-events.json').read_text(encoding='utf-8')).get('events', []))
        result = {
            'ok': True,
            'build_id': build_id,
            'started_at': started,
            'finished_at': datetime.now(timezone.utc).isoformat(),
            'archive': archive_path.name,
            'dist_root': str(dist_dir),
            'event_count': event_count,
        }
        RESULT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        shutil.rmtree(EXTRACT_ROOT, ignore_errors=True)
        shutil.rmtree(WORKING / 'node22', ignore_errors=True)
        finish_status(ok=True, message=f"static site build ready: {build_id}, events={event_count}")
        return 0
    except Exception as exc:
        finish_status(ok=False, message=f"{exc.__class__.__name__}: {exc}")
        raise


if __name__ == '__main__':
    raise SystemExit(main())
