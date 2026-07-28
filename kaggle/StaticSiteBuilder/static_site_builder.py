from __future__ import annotations

import base64
import hashlib
import importlib.util
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
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
PLAYWRIGHT_CHROMIUM_INSTALL_COMMAND = (
    'npx',
    'playwright',
    'install',
    '--with-deps',
    '--only-shell',
    'chromium',
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def validate_snapshot_input(db_path: Path, config: dict) -> dict:
    snapshot = config.get('snapshot') if isinstance(config.get('snapshot'), dict) else {}
    expected_sha = str(snapshot.get('sha256') or '').strip().lower()
    expected_size = int(snapshot.get('size') or 0)
    if config.get('profile') == 'production-candidate':
        if not snapshot.get('snapshot_id') or not re.fullmatch(r'[0-9a-f]{64}', expected_sha):
            raise RuntimeError('production snapshot identity/hash is missing')
        actual_sha = sha256_file(db_path)
        if actual_sha != expected_sha or (expected_size and db_path.stat().st_size != expected_size):
            raise RuntimeError('mounted production snapshot hash/size mismatch')
        with sqlite3.connect(str(db_path)) as con:
            quick_check = str(con.execute('pragma quick_check').fetchone()[0]).lower()
        if quick_check != 'ok':
            raise RuntimeError(f'mounted production snapshot quick_check failed: {quick_check}')
    return snapshot


def validate_build_clock(config: dict) -> dict:
    clock = config.get('build_clock') if isinstance(config.get('build_clock'), dict) else {}
    if clock.get('time_zone') != 'Europe/Kaliningrad':
        raise RuntimeError('static site build clock timezone mismatch')
    effective_date = str(clock.get('effective_date') or '')
    current_datetime = str(clock.get('current_datetime') or '')
    try:
        parsed = datetime.fromisoformat(current_datetime.replace('Z', '+00:00'))
    except ValueError as exc:
        raise RuntimeError('static site build clock datetime invalid') from exc
    if not effective_date or parsed.date().isoformat() != effective_date:
        raise RuntimeError('static site build clock date mismatch')
    if str(config.get('current_date') or '') != effective_date or str(config.get('current_datetime') or '') != current_datetime:
        raise RuntimeError('static site build clock/config mismatch')
    fingerprint = str(config.get('input_fingerprint') or '')
    if config.get('profile') == 'production-candidate' and not re.fullmatch(r'[0-9a-f]{64}', fingerprint):
        raise RuntimeError('production input fingerprint missing')
    return clock

def _load_status_loader(search_roots: list[Path] | None = None):
    """Load the callback helper from the mounted private status dataset.

    Kaggle dataset sources are mounted below ``/kaggle/input`` and are not
    automatically added to ``sys.path``.  A direct import therefore succeeds
    only for kernels that happen to bundle the helper themselves.  Search the
    mounted inputs explicitly so production static builds cannot silently run
    without heartbeat/terminal callbacks.
    """

    try:
        from kaggle_status_client import load_status_client as loader

        return loader
    except Exception as exc:  # pragma: no cover - expected on Kaggle mounts
        print(f'[kaggle_status] direct import failed: {exc}', flush=True)
    roots = search_roots or [ROOT, Path.cwd(), WORKING, INPUT_ROOT]
    seen: set[Path] = set()
    for root in roots:
        if not root.exists():
            continue
        candidates = [root / 'kaggle_status_client.py']
        try:
            candidates.extend(sorted(root.rglob('kaggle_status_client.py')))
        except Exception as exc:
            print(f'[kaggle_status] helper scan failed under {root}: {exc}', flush=True)
        for candidate in candidates:
            candidate = candidate.resolve()
            if candidate in seen or not candidate.is_file():
                continue
            seen.add(candidate)
            try:
                spec = importlib.util.spec_from_file_location(
                    'events_bot_static_site_kaggle_status_client', candidate
                )
                if spec is None or spec.loader is None:
                    continue
                module = importlib.util.module_from_spec(spec)
                sys.modules[spec.name] = module
                spec.loader.exec_module(module)
                loader = getattr(module, 'load_status_client', None)
                if callable(loader):
                    print(f'[kaggle_status] loaded helper from {candidate}', flush=True)
                    return loader
            except Exception as exc:
                print(f'[kaggle_status] helper load failed from {candidate}: {exc}', flush=True)
    return None


load_status_client = _load_status_loader()

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
    # Release exclusive resources before publishing the terminal event.  A
    # Kaggle worker can be torn down immediately after the terminal callback;
    # making report_written the final network operation prevents a completed
    # or failed build from leaving static_site:builder active for its full TTL.
    try:
        if STATUS_CLIENT is not None:
            for resource_key in list(ACQUIRED_RESOURCES):
                try:
                    STATUS_CLIENT.release_resource(resource_key)
                    ACQUIRED_RESOURCES.remove(resource_key)
                except Exception as exc:
                    print(f'[kaggle_status] resource release failed: {exc}', flush=True)
        if ok:
            status_event('report_written', phase='report', status='done', progress={'phase': 'report', 'progress_percent': 100, 'progress_label': 'готово'}, message=message)
        else:
            status_event('report_written', phase='failed', status='failed', progress={'phase': 'failed', 'progress_label': 'ошибка'}, message=message)
    finally:
        if STATUS_CLIENT is not None:
            STATUS_CLIENT.stop_alive()


def cleanup_transient_workspace() -> None:
    """Remove large throwaway dependencies without deleting recoverable outputs.

    Keep files that are useful after a failed long run, especially
    ``event_related_chain_cache.json`` and the copied SQLite DB.  The Node 22
    npm install can be hundreds of megabytes and must never be left in
    ``/kaggle/working`` where Kaggle publishes it as an output artifact.
    """

    transient_paths = [WORKING / 'node22']
    if EXTRACT_ROOT != WORKING:
        transient_paths.append(EXTRACT_ROOT)
    for path in transient_paths:
        try:
            if path.exists():
                print(f'[static-site-builder] cleaning transient path {path}', flush=True)
                shutil.rmtree(path, ignore_errors=True)
        except Exception as exc:
            print(f'[static-site-builder] transient cleanup failed for {path}: {exc}', flush=True)


def run(
    cmd: list[str],
    cwd: Path,
    env: dict[str, str] | None = None,
    *,
    timeout_seconds: int | None = None,
) -> None:
    print(f"[static-site-builder] $ {' '.join(cmd)} cwd={cwd}", flush=True)
    subprocess.run(
        cmd,
        cwd=str(cwd),
        env=env,
        check=True,
        timeout=timeout_seconds,
    )


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


def ensure_python_deps_for_bge(config: dict) -> None:
    if config.get('related_mode') != 'bge' and not config.get('unusual_enabled'):
        return
    probe = subprocess.run(
        [
            sys.executable, '-c',
            'import numpy, PIL, huggingface_hub; from FlagEmbedding import BGEM3FlagModel',
        ],
        cwd=str(WORKING),
        env=os.environ.copy(),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if probe.returncode == 0:
        return
    status_event(
        'alive',
        phase='semantic_preflight',
        status='alive',
        progress={
            'phase': 'semantic_preflight',
            'progress_percent': 10,
            'progress_label': 'установка pinned CPU BGE-M3 runtime',
        },
    )
    run(
        [
            sys.executable, '-m', 'pip', 'install', '--quiet',
            '--disable-pip-version-check',
            '--upgrade',
            # 1.4.0 is the first release with the Transformers 5 compatibility
            # required by the current Kaggle CPU image.  1.3.5 imports the
            # removed ``is_torch_fx_available`` helper and fails before BGE-M3
            # can load.
            'FlagEmbedding==1.4.0',
            'huggingface-hub>=0.28,<2',
            'Pillow>=10,<13',
        ],
        cwd=WORKING,
        env=os.environ.copy(),
    )


def ensure_python_deps_for_service_share() -> None:
    probe = subprocess.run(
        [sys.executable, '-c', 'from PIL import Image, ImageDraw, ImageFont'],
        cwd=str(WORKING),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if probe.returncode != 0:
        run(
            [sys.executable, '-m', 'pip', 'install', '--quiet', 'Pillow>=10,<13'],
            cwd=WORKING,
            env=os.environ.copy(),
        )


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
    snapshot = validate_snapshot_input(db_path, config)
    exporter = SITE_DIR / 'scripts' / 'export-production-preview-data.py'
    if not exporter.exists():
        raise FileNotFoundError(f'preview exporter missing: {exporter}')
    cache_filename = (config.get('related_cache_filename') or 'event_related_chain_cache.json').strip()
    input_cache = find_input_file(cache_filename)
    cache_path = WORKING / cache_filename
    if input_cache and not cache_path.exists():
        shutil.copy2(input_cache, cache_path)
    cache_inputs = {
        'bge_vector_cache_filename': 'static_event_bge_vectors.npz',
        'bge_vector_receipt_filename': 'static_event_bge_vectors.receipt.json',
        'unusual_cache_filename': 'unusual_events_cache.json',
        'unusual_last_good_filename': 'unusual_events_last_good.json',
    }
    semantic_paths: dict[str, Path] = {}
    for config_key, default_name in cache_inputs.items():
        filename = str(config.get(config_key) or default_name).strip()
        input_path = find_input_file(filename)
        working_path = WORKING / filename
        if input_path and not working_path.exists():
            shutil.copy2(input_path, working_path)
        semantic_paths[config_key] = working_path
    key_env = (config.get('gemma_related_key_env') or 'GOOGLE_API_KEY4').strip()
    if config.get('gemma_related_verify') or config.get('related_mode') == 'pgvector' or config.get('sync_pgvector_vectors'):
        ensure_python_deps_for_gemma(config)
        load_encrypted_secrets_to_env()
    if config.get('gemma_related_verify'):
        load_kaggle_secret_to_env(key_env)
        for secret_name in ['SUPABASE_URL', 'SUPABASE_KEY', 'SUPABASE_SERVICE_KEY', 'SUPABASE_SERVICE_ROLE_KEY']:
            load_kaggle_secret_to_env(secret_name)
    clock = validate_build_clock(config)
    cmd = [
        'python3',
        str(exporter),
        '--db', str(db_path),
        '--limit', str(config.get('limit') or 50),
        '--current-date', str(clock['effective_date']),
        '--output-dir', str(SITE_DIR / 'src/data'),
        '--catalog-mode', str(config.get('catalog_mode') or 'slice'),
        '--related-cache', str(cache_path),
        '--related-mode', str(config.get('related_mode') or 'sparse'),
        '--related-corpus-revision', str(config.get('related_corpus_revision') or ''),
        '--pgvector-embedding-model', str(config.get('pgvector_embedding_model') or 'gemini-embedding-2'),
        '--pgvector-embedding-key-env', str(config.get('pgvector_embedding_key_env') or 'GOOGLE_API_KEY4'),
        '--pgvector-max-provider-calls', str(config.get('pgvector_max_provider_calls') or 1000),
        '--bge-vector-cache', str(semantic_paths['bge_vector_cache_filename']),
        '--bge-vector-receipt', str(semantic_paths['bge_vector_receipt_filename']),
        '--bge-model-revision', str(config.get('bge_model_revision') or ''),
        '--bge-batch-size', str(config.get('bge_batch_size') or 8),
        '--unusual-cache', str(semantic_paths['unusual_cache_filename']),
        '--unusual-last-good', str(semantic_paths['unusual_last_good_filename']),
        '--site-origin', str(config.get('public_site_origin') or 'https://kenigevents.ru'),
        '--base-path', str(config.get('build_id') or ''),
        '--ics-base-url', str(config.get('ics_base_url') or ''),
        '--gemma-related-model', str(config.get('gemma_related_model') or 'models/gemma-4-26b-a4b-it'),
        '--gemma-related-key-env', key_env,
        '--gemma-related-max-anchors', str(config.get('gemma_related_max_anchors') or 0),
    ]
    if config.get('profile') == 'production-candidate':
        cmd.extend([
            '--repo-sha', str(config.get('repo_sha') or ''),
            '--run-id', str(config.get('run_id') or ''),
            '--build-id', str(config.get('build_id') or ''),
            '--snapshot-id', str(snapshot.get('snapshot_id') or ''),
            '--snapshot-sha256', str(snapshot.get('sha256') or ''),
            '--snapshot-size', str(snapshot.get('size') or 0),
            '--input-fingerprint', str(config.get('input_fingerprint') or ''),
        ])
    cmd.extend(['--current-datetime', str(clock['current_datetime'])])
    if config.get('focus_date_from'):
        cmd.extend(['--focus-date-from', str(config.get('focus_date_from'))])
    if config.get('focus_date_to'):
        cmd.extend(['--focus-date-to', str(config.get('focus_date_to'))])
    if config.get('sync_pgvector_vectors'):
        cmd.append('--sync-pgvector-vectors')
    if config.get('unusual_enabled'):
        cmd.append('--unusual-enabled')
    if config.get('unusual_migration'):
        cmd.append('--unusual-migration')
    if config.get('gemma_related_verify'):
        cmd.append('--gemma-related-verify')
    status_event('alive', phase='export', status='alive', progress={'phase': 'export', 'progress_percent': 18, 'progress_label': 'экспорт событий и related v2'})
    run(cmd, cwd=SITE_DIR, env=os.environ.copy())


def render_daily_service_share(config: dict, build_clock: dict) -> dict:
    scripts_dir = str(SITE_DIR / 'scripts')
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    from service_share_card import build_daily_service_share

    preview_path = SITE_DIR / 'src' / 'data' / 'preview-events.json'
    payload = json.loads(preview_path.read_text(encoding='utf-8'))
    events = payload.get('events') if isinstance(payload, dict) else None
    if not isinstance(events, list):
        raise RuntimeError('service share source snapshot has invalid event list')
    snapshot = config.get('snapshot') if isinstance(config.get('snapshot'), dict) else {}
    status_event(
        'alive',
        phase='service_share',
        status='alive',
        progress={
            'phase': 'service_share',
            'progress_percent': 22,
            'progress_label': 'ежедневная карточка сервиса 1080×1350',
        },
    )
    manifest = build_daily_service_share(
        events=events,
        public_root=SITE_DIR / 'public',
        build_id=str(config.get('build_id') or ''),
        measured_at=str(build_clock.get('current_datetime') or ''),
        source_snapshot_id=str(snapshot.get('snapshot_id') or '') or None,
        source_snapshot_hash=str(snapshot.get('sha256') or '') or None,
        input_fingerprint=str(config.get('input_fingerprint') or '') or None,
    )
    return {
        'status': 'ready',
        'asset_version': manifest.get('asset_version'),
        'manifest_payload_hash': manifest.get('manifest_payload_hash'),
        'local_date': manifest.get('local_date'),
        'fresh_until': manifest.get('fresh_until'),
        'width': int((manifest.get('assets') or {}).get('png', {}).get('width') or 0),
        'height': int((manifest.get('assets') or {}).get('png', {}).get('height') or 0),
    }


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


def apply_public_authorized_search_env(env: dict[str, str], config: dict) -> None:
    """Expose only browser-safe auth/search values to Astro.

    Runtime secrets are loaded into ``os.environ`` by the encrypted dataset
    path. Astro, however, receives the explicit ``env`` dict below, so we must
    copy the public Supabase URL/publishable key into that build environment.
    The service/secret key is intentionally never copied to a PUBLIC_* name.
    """

    public_url = (
        str(config.get('public_personalization_supabase_url') or '').strip()
        or os.environ.get('PUBLIC_PERSONALIZATION_SUPABASE_URL', '').strip()
        or os.environ.get('PERSONALIZATION_SUPABASE_URL', '').strip()
    )
    public_key = (
        str(config.get('public_personalization_supabase_publishable_key') or '').strip()
        or os.environ.get('PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY', '').strip()
        or os.environ.get('PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY', '').strip()
    )
    yandex_provider = (
        str(config.get('public_yandex_auth_provider') or '').strip()
        or os.environ.get('PUBLIC_YANDEX_AUTH_PROVIDER', '').strip()
        or 'custom:yandex'
    )
    search_transport = (
        str(config.get('public_authorized_search_transport') or '').strip()
        or os.environ.get('PUBLIC_AUTHORIZED_SEARCH_TRANSPORT', '').strip()
        or 'json'
    )
    if search_transport not in {'json', 'ndjson'}:
        raise RuntimeError('public_authorized_search_transport must be json or ndjson')
    if public_url:
        env['PUBLIC_PERSONALIZATION_SUPABASE_URL'] = public_url
    if public_key:
        env['PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY'] = public_key
    if yandex_provider:
        env['PUBLIC_YANDEX_AUTH_PROVIDER'] = yandex_provider
    env['PUBLIC_AUTHORIZED_SEARCH_TRANSPORT'] = search_transport


def apply_public_interest_clubs_env(env: dict[str, str]) -> None:
    """Copy the decrypted public club release flag into Astro's build env.

    ``env`` is intentionally snapshotted before the exporter loads the
    encrypted runtime bundle. Search has an explicit post-load bridge above;
    clubs need the same bridge or the exporter can write real club data while
    Astro still renders the fail-closed empty catalog.
    """

    public_enabled = os.environ.get('PUBLIC_INTEREST_CLUBS_ENABLED', '').strip()
    if public_enabled:
        env['PUBLIC_INTEREST_CLUBS_ENABLED'] = public_enabled


def apply_secret_candidate_research_env(env: dict[str, str], config: dict) -> None:
    """Bridge explicit review gates into the production-candidate build.

    The production root-form build receives the same process environment, but
    artifact rendering separately requires ``PUBLIC_SITE_MODE=secret_candidate``
    and therefore remains hard-disabled in production output.
    """

    if config.get('profile') != 'production-candidate':
        return
    if config.get('secret_candidate_artifact_research'):
        env['PUBLIC_ENABLE_AMBER_ARTIFACT_RESEARCH'] = 'tail'
    if config.get('secret_candidate_require_authorized_search'):
        env['SECRET_CANDIDATE_REQUIRE_AUTHORIZED_SEARCH'] = '1'


def run_production_preview_contract_gate(
    env: dict[str, str],
    *,
    build_id: str,
) -> dict[str, object]:
    """Run the legacy preview contract without making it a release artifact."""

    clean_build = re.sub(r'[^A-Za-z0-9._-]+', '-', str(build_id)).strip('-._')
    clean_build = clean_build[:96] or 'production-candidate'
    preview_build_id = f'preview-gate-{clean_build}'
    preview_env = dict(env)
    preview_env.update({
        'PREVIEW_BUILD_ID': preview_build_id,
        'PUBLIC_PREVIEW_BUILD_ID': preview_build_id,
        'SITE_BASE_PATH': f'/{preview_build_id}',
    })
    status_event(
        'alive',
        phase='check',
        status='alive',
        progress={
            'phase': 'check',
            'progress_percent': 36,
            'progress_label': 'legacy preview contract gate',
        },
    )
    run(['npm', 'run', 'build:preview'], cwd=SITE_DIR, env=preview_env)
    run(['npm', 'run', 'check:preview'], cwd=SITE_DIR, env=preview_env)
    preview_dist = SITE_DIR / 'dist' / preview_build_id
    if not preview_dist.is_dir():
        raise FileNotFoundError(f'preview contract output missing: {preview_dist}')
    return {
        'status': 'ok',
        'build_id': preview_build_id,
        'archived': False,
        'published': False,
    }


def main() -> int:
    started = datetime.now(timezone.utc).isoformat()
    try:
        # Status bootstrap belongs to the guarded lifecycle too.  In
        # particular, a blocked resource acquisition must still write a
        # terminal report instead of leaving a misleading "running" ledger.
        init_status()
        config = read_config()
        build_clock = validate_build_clock(config)
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
        apply_public_authorized_search_env(env, config)
        ensure_python_deps_for_service_share()
        ensure_python_deps_for_bge(config)
        export_preview_data_if_configured(config)
        service_share_result = render_daily_service_share(config, build_clock)
        semantic_result_path = SITE_DIR / 'src' / 'data' / 'static-semantic-build-result.json'
        if config.get('related_mode') == 'bge' or config.get('unusual_enabled'):
            if not semantic_result_path.is_file():
                raise RuntimeError('shared BGE/unusual semantic result is missing')
            semantic_result = json.loads(semantic_result_path.read_text(encoding='utf-8'))
            if (
                int(semantic_result.get('provider_calls', -1)) != 0
                or int(semantic_result.get('event_count') or -1) <= 0
                or not re.fullmatch(r'[0-9a-f]{64}', str(semantic_result.get('artifact_sha256') or ''))
                or not re.fullmatch(r'[0-9a-f]{64}', str(semantic_result.get('manifest_sha256') or ''))
            ):
                raise RuntimeError('shared BGE/unusual semantic result is partial or mismatched')
        else:
            semantic_result = {'status': 'disabled', 'provider_calls': 0}
        apply_public_authorized_search_env(env, config)
        apply_public_interest_clubs_env(env)
        apply_secret_candidate_research_env(env, config)
        env = ensure_node22(env)

        status_event('preflight_ok', phase='preflight', status='done', progress={'phase': 'preflight', 'progress_percent': 15, 'progress_label': 'окружение готово'})
        run(['node', '--version'], cwd=SITE_DIR, env=env)
        run(['npm', '--version'], cwd=SITE_DIR, env=env)
        status_event('alive', phase='install', status='alive', progress={'phase': 'install', 'progress_percent': 25, 'progress_label': 'установка npm зависимостей'})
        install_cmd = ['npm', 'ci', '--no-audit', '--no-fund'] if (SITE_DIR / 'package-lock.json').exists() else ['npm', 'install', '--no-audit', '--no-fund']
        run(install_cmd, cwd=SITE_DIR, env=env)
        profile = str(config.get('profile') or 'preview')
        if profile == 'production-candidate':
            status_event('alive', phase='install', status='alive', progress={'phase': 'install', 'progress_percent': 31, 'progress_label': 'установка pinned Chromium для release gate'})
            # Kaggle's CPU image does not include Playwright's Linux shared
            # libraries (for example libatk-1.0.so.0).  Installing only the
            # browser binary makes the mandatory gate fail before Chromium can
            # start.  Playwright's documented CI contract installs both the
            # pinned headless shell and its OS dependencies.
            run(list(PLAYWRIGHT_CHROMIUM_INSTALL_COMMAND), cwd=SITE_DIR, env=env)
        event_count = len(json.loads((SITE_DIR / 'src/data/preview-events.json').read_text(encoding='utf-8')).get('events', []))
        artifacts: list[dict] = []
        result_details: dict = {}
        if profile == 'production-candidate':
            repo_sha = str(config.get('repo_sha') or '')
            run_id = str(config.get('run_id') or '')
            token = str(config.get('candidate_token') or '')
            if not re.fullmatch(r'[0-9a-f]{40}', repo_sha) or not run_id or not re.fullmatch(r'[A-Za-z0-9_-]{43}', token):
                raise RuntimeError('production build identity/candidate token is invalid')
            env.update({
                'STATIC_SITE_REPO_SHA': repo_sha,
                'STATIC_SITE_RUN_ID': run_id,
                'PRODUCTION_BUILD_ID': str(build_id),
                'SECRET_CANDIDATE_TOKEN': token,
            })
            browser_evidence_dir = WORKING / f'{build_id}-browser-evidence'
            root_browser_evidence = browser_evidence_dir / 'production-root'
            candidate_browser_evidence = browser_evidence_dir / 'secret-candidate'
            root_browser_evidence.mkdir(parents=True, exist_ok=True)
            candidate_browser_evidence.mkdir(parents=True, exist_ok=True)
            preview_contract = run_production_preview_contract_gate(
                env,
                build_id=str(build_id),
            )
            status_event('alive', phase='build', status='alive', progress={'phase': 'build', 'progress_percent': 42, 'progress_label': 'production root-form build'})
            run(['npm', 'run', 'build:production'], cwd=SITE_DIR, env=env)
            root_dist = SITE_DIR / 'dist'
            preview_gate_path = root_dist / str(preview_contract['build_id'])
            if preview_gate_path.exists():
                raise RuntimeError(
                    'production build did not clear ephemeral preview contract output'
                )
            status_event('alive', phase='check', status='alive', progress={'phase': 'check', 'progress_percent': 57, 'progress_label': 'Chromium gate production root'})
            run([
                'npm', 'run', 'check:browser-release', '--',
                '--root', str(root_dist),
                '--manifest', str(root_dist / 'static-release-manifest.json'),
                '--report', str(root_browser_evidence / 'browser-release-report.json'),
                '--artifact-dir', str(root_browser_evidence),
            ], cwd=SITE_DIR, env=env, timeout_seconds=300)
            root_manifest = json.loads((root_dist / 'static-release-manifest.json').read_text(encoding='utf-8'))
            root_archive = WORKING / f'{build_id}-root.tar.gz'
            with tarfile.open(root_archive, 'w:gz') as tar:
                tar.add(root_dist, arcname=f'{build_id}-root')
            artifacts.append({'kind': 'production_root', 'filename': root_archive.name, 'sha256': sha256_file(root_archive), 'size': root_archive.stat().st_size})

            status_event('alive', phase='build', status='alive', progress={'phase': 'build', 'progress_percent': 68, 'progress_label': 'noindex secret candidate build'})
            run(['npm', 'run', 'build:secret-candidate'], cwd=SITE_DIR, env=env)
            candidate_dist = SITE_DIR / 'dist' / '_review' / token
            status_event('alive', phase='check', status='alive', progress={'phase': 'check', 'progress_percent': 81, 'progress_label': 'Chromium gate secret candidate'})
            run([
                'npm', 'run', 'check:browser-release', '--',
                '--root', str(candidate_dist),
                '--manifest', str(candidate_dist / 'secret-candidate-manifest.json'),
                '--report', str(candidate_browser_evidence / 'browser-release-report.json'),
                '--artifact-dir', str(candidate_browser_evidence),
            ], cwd=SITE_DIR, env=env, timeout_seconds=300)
            candidate_manifest = json.loads((candidate_dist / 'secret-candidate-manifest.json').read_text(encoding='utf-8'))
            candidate_archive = WORKING / f'{build_id}-secret-candidate.tar.gz'
            with tarfile.open(candidate_archive, 'w:gz') as tar:
                tar.add(candidate_dist, arcname=f'_review/{token}')
            artifacts.append({'kind': 'secret_candidate', 'filename': candidate_archive.name, 'sha256': sha256_file(candidate_archive), 'size': candidate_archive.stat().st_size})
            browser_evidence_archive = WORKING / f'{build_id}-browser-evidence.tar.gz'
            with tarfile.open(browser_evidence_archive, 'w:gz') as tar:
                tar.add(browser_evidence_dir, arcname='browser-evidence')
            artifacts.append({'kind': 'browser_evidence', 'filename': browser_evidence_archive.name, 'sha256': sha256_file(browser_evidence_archive), 'size': browser_evidence_archive.stat().st_size})
            result_details = {
                'repo_sha': repo_sha,
                'run_id': run_id,
                'snapshot': {
                    'snapshot_id': str((config.get('snapshot') or {}).get('snapshot_id') or ''),
                    'snapshot_sha256': str((config.get('snapshot') or {}).get('sha256') or ''),
                    'size': int((config.get('snapshot') or {}).get('size') or 0),
                    'max_event_revision': root_manifest.get('snapshot', {}).get('max_event_revision'),
                },
                'candidate': {'token': token, 'token_sha256': candidate_manifest.get('token_sha256'), 'base_path': candidate_manifest.get('base_path')},
                'counts': root_manifest.get('counts'),
                'checks': {
                    'preview_contract': preview_contract,
                    'production': root_manifest.get('checks'),
                    'secret_candidate': candidate_manifest.get('checks'),
                },
                'related_revision': root_manifest.get('versions', {}).get('related'),
                'tree_sha256': {'production': root_manifest.get('tree_sha256'), 'secret_candidate': candidate_manifest.get('tree_sha256')},
                'semantic': semantic_result,
                'service_share': service_share_result,
            }
        else:
            status_event('alive', phase='build', status='alive', progress={'phase': 'build', 'progress_percent': 45, 'progress_label': 'Astro preview build'})
            run(['npm', 'run', 'build:preview'], cwd=SITE_DIR, env=env)
            status_event('alive', phase='check', status='alive', progress={'phase': 'check', 'progress_percent': 75, 'progress_label': 'проверка preview'})
            run(['npm', 'run', 'check:preview'], cwd=SITE_DIR, env=env)
            dist_dir = SITE_DIR / 'dist' / build_id
            if not dist_dir.exists():
                raise FileNotFoundError(f"build output missing: {dist_dir}")
            archive_path = WORKING / f'{build_id}.tar.gz'
            with tarfile.open(archive_path, 'w:gz') as tar:
                tar.add(dist_dir, arcname=build_id)
            artifacts.append({'kind': 'preview', 'filename': archive_path.name, 'sha256': sha256_file(archive_path), 'size': archive_path.stat().st_size})
            # Preserve the v1 preview handoff fields while the richer v2
            # artifact list is adopted by downstream consumers.
            result_details = {
                'archive': archive_path.name,
                'dist_root': str(dist_dir),
                'semantic': semantic_result,
                'service_share': service_share_result,
            }
        status_event('alive', phase='archive', status='alive', progress={'phase': 'archive', 'progress_percent': 92, 'progress_label': 'артефакты проверены'})
        result = {
            'schema_version': 'static_site_build_result_v2',
            'ok': True, 'profile': profile, 'build_id': build_id,
            'started_at': started, 'finished_at': datetime.now(timezone.utc).isoformat(),
            'event_count': event_count, 'artifacts': artifacts, 'failure_class': None,
            'input_fingerprint': config.get('input_fingerprint'),
            'build_clock': build_clock,
            **result_details,
        }
        RESULT_PATH.write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        print(json.dumps(result, ensure_ascii=False, indent=2), flush=True)
        cleanup_transient_workspace()
        finish_status(ok=True, message=f"static site build ready: {build_id}, events={event_count}")
        return 0
    except Exception as exc:
        failure = {
            'schema_version': 'static_site_build_result_v2', 'ok': False,
            'build_id': locals().get('build_id'), 'profile': str(locals().get('config', {}).get('profile') or 'preview'),
            'started_at': started, 'finished_at': datetime.now(timezone.utc).isoformat(),
            'failure_class': 'permanent_input' if isinstance(exc, (ValueError, FileNotFoundError)) else 'retryable_build',
            'input_fingerprint': locals().get('config', {}).get('input_fingerprint'),
            'build_clock': locals().get('config', {}).get('build_clock'),
            'error_type': exc.__class__.__name__, 'error': str(exc)[:1000],
        }
        try:
            WORKING.mkdir(parents=True, exist_ok=True)
            RESULT_PATH.write_text(json.dumps(failure, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
        except Exception:
            pass
        cleanup_transient_workspace()
        finish_status(ok=False, message=f"{exc.__class__.__name__}: {exc}")
        raise


if __name__ == '__main__':
    raise SystemExit(main())
