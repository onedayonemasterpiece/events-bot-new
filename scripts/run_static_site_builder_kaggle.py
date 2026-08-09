#!/usr/bin/env python3
"""Stage, push and optionally wait for the Kaggle CPU static-site builder.

The notebook/kernel receives a bounded Astro site source tree with already
exported `site/src/data/preview-*.json`. It builds and checks the static site on
CPU, then returns a tar.gz artifact; publishing to Object Storage/CDN remains a
single Fly-side step guarded by the outbox/coalescing lock.
"""
from __future__ import annotations

import argparse
import asyncio
import base64
import fcntl
import hashlib
import json
import os
import re
import secrets
import shutil
import subprocess
import sys
import tarfile
import tempfile
import time
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from runtime_disk import runtime_scratch_health, writable_disk_health
from static_site_release import (
    STATIC_SITE_IMAGE_SOURCE_MANIFEST_SCHEMA,
    STATIC_SITE_SOURCE_IDENTITY_SCHEMA,
    resolve_build_clock,
    static_site_artifact_root,
    static_site_scratch_root,
    validate_static_site_image_source_manifest,
    validate_static_site_source_identity,
)
KERNEL_SRC = ROOT / 'kaggle' / 'StaticSiteBuilder'
SITE_SOURCE_REPO_CONTRACTS = (
    Path('docs/testing/transport-fault-profiles.v1.yml'),
)
IMAGE_SOURCE_ROOTS = (
    Path('site'),
    Path('google_ai'),
    Path('kaggle/StaticSiteBuilder'),
)
IMAGE_SOURCE_FILES = (
    Path('scripts/run_static_site_builder_kaggle.py'),
    Path('scripts/sync_event_search_vectors_to_supabase.py'),
    Path('scripts/check_static_collections_product_quality.py'),
    Path('tests/fixtures/unusual_events_golden_v1.json'),
    *SITE_SOURCE_REPO_CONTRACTS,
)
SOURCE_IGNORED_PARTS = {
    'node_modules', 'dist', '.astro', '.vercel', '__pycache__', 'output',
}
SITE_SRC = ROOT / 'site'
ARTIFACT_ROOT = static_site_artifact_root(ROOT)
SCRATCH_ROOT = static_site_scratch_root(ARTIFACT_ROOT)
LOCK_PATH = ARTIFACT_ROOT / 'static-site-kaggle.lock'
ADOPT_REMOTE_LIVE_EXIT = 75
ADOPT_REMOTE_UNAVAILABLE_EXIT = 76
BUILD_ID_RE = re.compile(r'(?:preview|production)-[A-Za-z0-9][A-Za-z0-9._-]{0,191}')
SCRATCH_DIR_RE = re.compile(r'static-site-kaggle-[A-Za-z0-9_-]+')


def _env_nonnegative_int(name: str, default: int) -> int:
    raw = (os.getenv(name) or '').strip()
    try:
        return max(0, int(raw) if raw else int(default))
    except (TypeError, ValueError):
        return max(0, int(default))


def collection_semantic_compute_required(args: argparse.Namespace) -> bool:
    """Production candidates always compute, regardless of publication flags."""

    return bool(
        getattr(args, 'collection_semantic_compute', False)
        or getattr(args, 'profile', 'preview') == 'production-candidate'
    )


def require_static_site_storage_ready() -> None:
    """Fail before Kaggle submission when root temp or durable work is unusable."""

    root_scratch = runtime_scratch_health()
    if (
        root_scratch.get('status') not in {'ok', 'warning'}
        or root_scratch.get('tempfile_status') != 'ok'
    ):
        raise RuntimeError(
            'root scratch preflight failed: '
            f"status={root_scratch.get('status')} "
            f"error={root_scratch.get('tempfile_error') or root_scratch.get('error') or 'none'}"
        )
    SCRATCH_ROOT.mkdir(parents=True, exist_ok=True)
    work = writable_disk_health(
        SCRATCH_ROOT,
        warn_free_mb=_env_nonnegative_int('STATIC_SITE_STORAGE_WARN_FREE_MB', 1536),
        critical_free_mb=_env_nonnegative_int('STATIC_SITE_STORAGE_CRITICAL_FREE_MB', 1024),
        tempfile_probe=True,
    )
    if work.get('status') not in {'ok', 'warning'} or work.get('tempfile_status') != 'ok':
        raise RuntimeError(
            'static-site storage preflight failed: '
            f"status={work.get('status')} "
            f"error={work.get('tempfile_error') or work.get('error') or 'none'}"
        )


def prune_abandoned_static_site_scratch(scratch_root: Path = SCRATCH_ROOT) -> dict[str, object]:
    """Remove only runner-owned scratch trees while the process lock is held.

    ``TemporaryDirectory`` handles normal exits.  A killed Fly process can
    leave the staged SQLite dataset behind, however, and that residue can make
    the next capacity probe fail before it gets a chance to recover.  The
    caller must hold ``LOCK_PATH``; therefore no conforming local runner can be
    using one of these directories at the same time.
    """

    root = Path(scratch_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    removed: list[str] = []
    removed_bytes = 0
    for candidate in root.iterdir():
        if (
            candidate.is_symlink()
            or not candidate.is_dir()
            or not SCRATCH_DIR_RE.fullmatch(candidate.name)
        ):
            continue
        size = 0
        for parent, directories, files in os.walk(candidate, followlinks=False):
            directories[:] = [
                name for name in directories if not (Path(parent) / name).is_symlink()
            ]
            for name in files:
                path = Path(parent) / name
                if path.is_symlink():
                    continue
                try:
                    size += path.stat().st_size
                except FileNotFoundError:
                    continue
        shutil.rmtree(candidate)
        removed.append(candidate.name)
        removed_bytes += size
    return {
        'removed_directories': sorted(removed),
        'removed_bytes': removed_bytes,
    }


def prepare_output_directory(artifact_root: Path, build_id: str) -> Path:
    """Create one exact runner output directory without path/symlink escape."""

    clean_build_id = str(build_id or '').strip()
    if not BUILD_ID_RE.fullmatch(clean_build_id):
        raise ValueError('static-site output build id is invalid')
    root = Path(artifact_root).resolve()
    root.mkdir(parents=True, exist_ok=True)
    candidate = root / f'output-{clean_build_id}'
    if candidate.parent != root:
        raise ValueError('static-site output path escaped artifact root')
    if candidate.is_symlink():
        raise RuntimeError('static-site output symlink is refused')
    if candidate.exists():
        if not candidate.is_dir():
            raise RuntimeError('static-site output path is not a directory')
        shutil.rmtree(candidate)
    candidate.mkdir(mode=0o700)
    return candidate


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def _source_file_entries(
    roots: list[tuple[Path, str]],
    *,
    optional_files: list[tuple[Path, str]] | None = None,
) -> list[tuple[str, Path]]:
    entries: list[tuple[str, Path]] = []
    for source_root, logical_root in roots:
        source_root = source_root.resolve()
        if not source_root.is_dir():
            raise FileNotFoundError(source_root)
        for path in sorted(source_root.rglob('*')):
            relative = path.relative_to(source_root)
            if any(part in SOURCE_IGNORED_PARTS for part in relative.parts):
                continue
            if path.is_symlink():
                raise RuntimeError(f'static-site source symlink is refused: {logical_root}/{relative.as_posix()}')
            if not path.is_file() or path.name.endswith(('.pyc', '.DS_Store')):
                continue
            entries.append((f'{logical_root}/{relative.as_posix()}', path))
    for path, logical_path in optional_files or []:
        path = path.resolve()
        if not path.exists():
            continue
        if path.is_symlink() or not path.is_file():
            raise RuntimeError(f'static-site source file is not regular: {logical_path}')
        entries.append((logical_path, path))
    logical_paths = [logical for logical, _path in entries]
    if len(set(logical_paths)) != len(logical_paths):
        raise RuntimeError('static-site source manifest contains duplicate logical paths')
    return sorted(entries, key=lambda item: item[0])


def _source_tree_digest(entries: list[tuple[str, Path]]) -> tuple[str, int]:
    digest = hashlib.sha256()
    for logical_path, path in entries:
        size = path.stat().st_size
        file_sha256 = sha256_file(path)
        digest.update(
            json.dumps(
                [logical_path, size, file_sha256],
                ensure_ascii=False,
                separators=(',', ':'),
            ).encode('utf-8')
        )
        digest.update(b'\n')
    return digest.hexdigest(), len(entries)


def build_image_source_manifest(
    repo_sha: str,
    *,
    root: Path | None = None,
) -> dict[str, object]:
    root = (root or ROOT).resolve()
    clean_sha = resolve_repo_sha(repo_sha)
    roots = [(root / relative, relative.as_posix()) for relative in IMAGE_SOURCE_ROOTS]
    files = [
        (root / relative, relative.as_posix())
        for relative in IMAGE_SOURCE_FILES
    ]
    source_tree_sha256, source_file_count = _source_tree_digest(
        _source_file_entries(roots, optional_files=files)
    )
    return {
        'schema_version': STATIC_SITE_IMAGE_SOURCE_MANIFEST_SCHEMA,
        'repo_sha': clean_sha,
        'source_tree_sha256': source_tree_sha256,
        'source_file_count': source_file_count,
    }


def _manifest_bytes(value: dict[str, object]) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
        + '\n'
    ).encode('utf-8')


def write_image_source_manifest(
    path: Path,
    *,
    repo_sha: str,
    root: Path | None = None,
) -> dict[str, object]:
    manifest = build_image_source_manifest(repo_sha, root=root)
    target = path.resolve()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(_manifest_bytes(manifest))
    return manifest


def resolve_image_source_contract(args: argparse.Namespace) -> dict[str, object]:
    repo_sha = str(getattr(args, 'repo_sha', '') or '').strip().lower()
    configured = str(getattr(args, 'image_source_manifest', '') or '').strip()
    if configured:
        path = Path(configured).resolve()
        if not path.is_file() or path.stat().st_size > 64 * 1024:
            raise RuntimeError('static-site image source manifest is missing or unbounded')
        raw = path.read_bytes()
        try:
            manifest = json.loads(raw)
        except Exception as exc:
            raise RuntimeError(f'static-site image source manifest is invalid: {exc}') from exc
        validated = validate_static_site_image_source_manifest(manifest, repo_sha=repo_sha)
        actual = build_image_source_manifest(repo_sha)
        if validated != actual:
            raise RuntimeError('static-site image source bytes differ from the build-time manifest')
    else:
        if os.getenv('FLY_APP_NAME') or os.getenv('FLY_MACHINE_ID'):
            raise RuntimeError('Fly static-site build requires the baked image source manifest')
        validated = build_image_source_manifest(repo_sha)
        raw = _manifest_bytes(validated)
    manifest_sha256 = hashlib.sha256(raw).hexdigest()
    expected_sha256 = str(
        getattr(args, 'expected_image_source_manifest_sha256', '') or ''
    ).strip().lower()
    if expected_sha256 and manifest_sha256 != expected_sha256:
        raise RuntimeError('static-site image source manifest digest mismatch')
    return {
        **validated,
        'manifest_sha256': manifest_sha256,
    }


def payload_source_tree_digest(site_dir: Path) -> str:
    roots = [(site_dir, 'site')]
    files = [
        (ROOT / relative, relative.as_posix())
        for relative in SITE_SOURCE_REPO_CONTRACTS
    ]
    digest, _count = _source_tree_digest(
        _source_file_entries(roots, optional_files=files)
    )
    return digest


def validate_search_corpus_receipt(path: Path) -> dict:
    """Validate the non-secret, vector-owner receipt before Kaggle handoff."""

    if not path.is_file():
        raise FileNotFoundError(f'Search corpus receipt is missing: {path}')
    if path.stat().st_size > 8 * 1024 * 1024:
        raise RuntimeError('Search corpus receipt exceeds the bounded handoff size')
    try:
        receipt = json.loads(path.read_text(encoding='utf-8'))
    except Exception as exc:
        raise RuntimeError(f'Search corpus receipt is invalid JSON: {exc}') from exc
    if not isinstance(receipt, dict):
        raise RuntimeError('Search corpus receipt must be one JSON object')
    if receipt.get('schema_version') != 'event_vector_sync_receipt_v2':
        raise RuntimeError('Search corpus receipt v2 is required')
    if receipt.get('status') not in {'complete', 'success'} or receipt.get('complete') is False:
        raise RuntimeError('Search corpus receipt is incomplete')
    for key in (
        'catalog_revision',
        'corpus_revision',
        'search_document_revision',
        'search_v3_hash',
        'related_v1_hash',
    ):
        if not re.fullmatch(r'[0-9a-f]{64}', str(receipt.get(key) or '')):
            raise RuntimeError(f'Search corpus receipt has invalid {key}')
    if receipt['corpus_revision'] != receipt['search_v3_hash']:
        raise RuntimeError('Search corpus receipt corpus/search_v3 mismatch')
    if receipt['search_document_revision'] != receipt['corpus_revision']:
        raise RuntimeError('Search corpus receipt search-document revision mismatch')
    coverage = receipt.get('coverage')
    if not isinstance(coverage, dict) or coverage.get('status') != 'complete':
        raise RuntimeError('Search corpus receipt coverage is incomplete')
    revisions = receipt.get('event_revisions')
    if not isinstance(revisions, dict):
        raise RuntimeError('Search corpus receipt event revisions are missing')
    return receipt


def atomic_copy_file(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    temporary = target.with_name(f'.{target.name}.{os.getpid()}.tmp')
    shutil.copy2(source, temporary)
    with temporary.open('rb') as handle:
        os.fsync(handle.fileno())
    os.replace(temporary, target)


def persist_semantic_outputs(
    out_dir: Path, args: argparse.Namespace, result: dict[str, object]
) -> None:
    semantic = result.get('semantic')
    semantic = semantic if isinstance(semantic, dict) else {}
    outputs = [
        ('static_event_bge_vectors.npz', args.bge_vector_cache, 'vector_cache_sha256'),
        ('static_event_bge_vectors.receipt.json', args.bge_vector_receipt, 'vector_receipt_sha256'),
    ]
    if getattr(args, 'unusual_enabled', False) or semantic.get('unusual_cache_sha256'):
        outputs.extend([
            ('unusual_events_cache.json', args.unusual_cache, 'unusual_cache_sha256'),
            ('unusual_events_last_good.json', args.unusual_last_good, 'last_good_sha256'),
        ])
    if collection_semantic_compute_required(args):
        product_parent = Path(args.collection_product_snapshot).parent
        outputs.extend([
            ('collection-batch-v1.json', args.collection_batch, 'collection_batch_sha256'),
            (
                'static-collection-product-snapshot-v1.json',
                args.collection_product_snapshot,
                'collection_product_snapshot_sha256',
            ),
            (
                'static-collections-product-quality.json',
                str(product_parent / 'static-collections-product-quality.json'),
                'collection_product_quality_report_sha256',
            ),
            (
                'static-collections-product-quality.md',
                str(product_parent / 'static-collections-product-quality.md'),
                'collection_product_quality_markdown_sha256',
            ),
            (
                'qa-summary.json',
                str(product_parent / 'qa-summary.json'),
                'collection_product_quality_qa_summary_sha256',
            ),
            (
                'collection-batch-last-good.json',
                args.collection_batch_last_good,
                'collection_last_good_sha256',
            ),
        ])
    for filename, target_value, hash_key in outputs:
        if not target_value:
            continue
        source = out_dir / filename
        expected_hash = str(semantic.get(hash_key) or '')
        if not source.is_file():
            if hash_key in {'last_good_sha256', 'collection_last_good_sha256'} and not expected_hash:
                continue
            raise RuntimeError(f'validated semantic cache output missing: {filename}')
        if not re.fullmatch(r'[0-9a-f]{64}', expected_hash) or sha256_file(source) != expected_hash:
            raise RuntimeError(f'semantic cache output hash mismatch: {filename}')
        target = Path(target_value)
        atomic_copy_file(source, target)
        print(f'[static-site-kaggle] semantic cache persisted atomically: {target}', flush=True)


def load_snapshot_contract(args: argparse.Namespace) -> dict[str, object]:
    if args.profile == 'preview':
        return {}
    if not args.db or not args.snapshot_manifest:
        raise ValueError('production-candidate profile requires --db and --snapshot-manifest')
    db_path = Path(args.db).resolve()
    manifest_path = Path(args.snapshot_manifest).resolve()
    manifest = json.loads(manifest_path.read_text(encoding='utf-8'))
    snapshot = manifest.get('snapshot') if isinstance(manifest.get('snapshot'), dict) else manifest
    snapshot_id = str(snapshot.get('snapshot_id') or manifest.get('snapshot_id') or '').strip()
    expected_sha = str(snapshot.get('sha256') or snapshot.get('snapshot_sha256') or manifest.get('snapshot_sha256') or '').strip().lower()
    expected_size = int(snapshot.get('size') or snapshot.get('size_bytes') or manifest.get('snapshot_size') or 0)
    quick_check = str(snapshot.get('quick_check') or manifest.get('quick_check') or '').strip().lower()
    if not snapshot_id or not re.fullmatch(r'[0-9a-f]{64}', expected_sha):
        raise ValueError('snapshot manifest requires snapshot_id and SHA-256')
    actual_sha = sha256_file(db_path)
    actual_size = db_path.stat().st_size
    if actual_sha != expected_sha or (expected_size and actual_size != expected_size):
        raise ValueError('immutable snapshot hash/size does not match its manifest')
    if quick_check and quick_check not in {'ok', 'passed'}:
        raise ValueError(f'snapshot quick_check is not ok: {quick_check}')
    return {'snapshot_id': snapshot_id, 'sha256': actual_sha, 'size': actual_size, 'quick_check': quick_check or 'ok'}


def validate_downloaded_result(out_dir: Path, args: argparse.Namespace) -> dict[str, object]:
    result_path = out_dir / 'static_site_build_result.json'
    if not result_path.exists() or result_path.stat().st_size > 256 * 1024:
        raise RuntimeError('Kaggle result JSON is missing or unbounded')
    result = json.loads(result_path.read_text(encoding='utf-8'))
    if result.get('ok') is not True or result.get('build_id') != args.build_id:
        raise RuntimeError('Kaggle result build identity/status mismatch')
    if args.profile == 'production-candidate':
        expected = {
            'run_id': args.run_id,
            'repo_sha': args.repo_sha,
            'snapshot_id': args.snapshot_contract['snapshot_id'],
            'snapshot_sha256': args.snapshot_contract['sha256'],
            'profile': args.profile,
        }
        for key, value in expected.items():
            actual = result.get('snapshot', {}).get(key) if key.startswith('snapshot_') else result.get(key)
            if str(actual) != str(value):
                raise RuntimeError(f'Kaggle result {key} mismatch: {actual!r} != {value!r}')
        image_source = getattr(args, 'image_source_contract', None) or resolve_image_source_contract(args)
        try:
            validate_static_site_source_identity(
                result.get('source') if isinstance(result.get('source'), dict) else {},
                repo_sha=args.repo_sha,
                image_source_manifest_sha256=str(image_source['manifest_sha256']),
                image_source_tree_sha256=str(image_source['source_tree_sha256']),
            )
        except Exception as exc:
            raise RuntimeError(f'Kaggle result source identity mismatch: {exc}') from exc
        if result.get('input_fingerprint') != args.input_fingerprint:
            raise RuntimeError('Kaggle result input fingerprint mismatch')
        if result.get('build_clock') != args.build_clock:
            raise RuntimeError('Kaggle result build clock mismatch')
        if collection_semantic_compute_required(args):
            semantic = result.get('semantic')
            if not isinstance(semantic, dict):
                raise RuntimeError('Kaggle collection semantic result metadata missing')
            if (
                int(semantic.get('provider_calls', -1)) != 0
                or int(semantic.get('event_count') or -1) <= 0
                or not re.fullmatch(r'[0-9a-f]{64}', str(semantic.get('artifact_sha256') or ''))
                or not re.fullmatch(r'[0-9a-f]{64}', str(semantic.get('collection_batch_sha256') or ''))
                or not re.fullmatch(r'[0-9a-f]{64}', str(semantic.get('collection_product_snapshot_sha256') or ''))
                or not re.fullmatch(r'[0-9a-f]{64}', str(semantic.get('collection_product_input_fingerprint') or ''))
                or not re.fullmatch(r'[0-9a-f]{64}', str(semantic.get('collection_product_normalized_output_sha256') or ''))
                or int(semantic.get('collection_product_provider_calls', -1)) != 0
                or semantic.get('collection_product_quality_status') not in {'HEALTHY', 'WATCH'}
                or not re.fullmatch(r'[0-9a-f]{64}', str(semantic.get('collection_product_quality_report_sha256') or ''))
                or not re.fullmatch(r'[0-9a-f]{64}', str(semantic.get('collection_product_quality_markdown_sha256') or ''))
                or not re.fullmatch(r'[0-9a-f]{64}', str(semantic.get('collection_product_quality_qa_summary_sha256') or ''))
            ):
                raise RuntimeError('Kaggle collection semantic result metadata mismatch')
            for filename, hash_key in (
                ('static-collections-product-quality.json', 'collection_product_quality_report_sha256'),
                ('static-collections-product-quality.md', 'collection_product_quality_markdown_sha256'),
                ('qa-summary.json', 'collection_product_quality_qa_summary_sha256'),
            ):
                report_path = out_dir / filename
                if not report_path.is_file() or sha256_file(report_path) != semantic.get(hash_key):
                    raise RuntimeError(f'Kaggle collection product-quality output mismatch: {filename}')
        elif args.related_mode == 'bge' or getattr(args, 'unusual_enabled', False):
            semantic = result.get('semantic')
            if (
                not isinstance(semantic, dict)
                or int(semantic.get('provider_calls', -1)) != 0
                or int(semantic.get('event_count') or -1) <= 0
                or not re.fullmatch(r'[0-9a-f]{64}', str(semantic.get('artifact_sha256') or ''))
                or not re.fullmatch(r'[0-9a-f]{64}', str(semantic.get('manifest_sha256') or ''))
            ):
                raise RuntimeError('Kaggle shared BGE/unusual result metadata mismatch')
        service_share = result.get('service_share')
        if (
            not isinstance(service_share, dict)
            or service_share.get('status') != 'ready'
            or service_share.get('local_date') != args.build_clock.get('effective_date')
            or int(service_share.get('width') or 0) != 1080
            or int(service_share.get('height') or 0) != 1350
            or not re.fullmatch(
                r'[0-9a-f]{64}',
                str(service_share.get('manifest_payload_hash') or ''),
            )
        ):
            raise RuntimeError('Kaggle daily service-share result metadata mismatch')
        artifacts = result.get('artifacts')
        expected_artifact_kinds = {'production_root', 'secret_candidate', 'browser_evidence'}
        actual_artifact_kinds = {
            str(artifact.get('kind') or '')
            for artifact in artifacts
        } if isinstance(artifacts, list) else set()
        if not isinstance(artifacts, list) or len(artifacts) != 3 or actual_artifact_kinds != expected_artifact_kinds:
            raise RuntimeError('production-candidate result must contain root, secret and browser-evidence artifacts')
        for artifact in artifacts:
            name = str(artifact.get('filename') or '')
            path = out_dir / name
            if not name.endswith('.tar.gz') or not path.is_file():
                raise RuntimeError(f'Kaggle artifact missing: {name}')
            if path.stat().st_size != int(artifact.get('size') or -1) or sha256_file(path) != artifact.get('sha256'):
                raise RuntimeError(f'Kaggle artifact hash/size mismatch: {name}')
        token = str(result.get('candidate', {}).get('token') or '')
        if token != args.candidate_token or not re.fullmatch(r'[A-Za-z0-9_-]{43}', token):
            raise RuntimeError('Kaggle candidate token mismatch')
    return result


def copy_tree(src: Path, dst: Path, *, ignore_extra: list[str] | None = None) -> None:
    ignore = shutil.ignore_patterns(
        'node_modules', 'dist', '.astro', '.vercel', '__pycache__', '*.pyc', '.DS_Store',
        *(ignore_extra or []),
    )
    shutil.copytree(src, dst, ignore=ignore, dirs_exist_ok=True)


def run(cmd: list[str], cwd: Path = ROOT, env: dict[str, str] | None = None) -> None:
    print(f"[static-site-kaggle] $ {' '.join(cmd)}", flush=True)
    subprocess.run(cmd, cwd=str(cwd), env=env, check=True)


def resolve_build_template(value: str | None, build_id: str) -> str | None:
    if not value:
        return None
    resolved = str(value).replace('{buildId}', build_id)
    if '{' in resolved or '}' in resolved:
        raise ValueError(f'unresolved build template: {value}')
    return resolved


def resolve_repo_sha(value: str | None) -> str:
    configured = str(value or '').strip().lower()
    if not configured:
        completed = subprocess.run(
            ['git', 'rev-parse', 'HEAD'],
            cwd=str(ROOT),
            check=True,
            capture_output=True,
            text=True,
        )
        configured = completed.stdout.strip().lower()
    if not re.fullmatch(r'[0-9a-f]{40}', configured):
        raise ValueError('static-site builder requires a full 40-character repo SHA')
    return configured


def tar_site_source(site_dir: Path, archive_path: Path) -> None:
    def tar_filter(info: tarfile.TarInfo) -> tarfile.TarInfo | None:
        parts = Path(info.name).parts
        ignored = {'node_modules', 'dist', '.astro', '.vercel', '__pycache__'}
        if any(part in ignored for part in parts):
            return None
        if info.name.endswith('.pyc') or info.name.endswith('.DS_Store'):
            return None
        return info

    with tarfile.open(archive_path, 'w:gz') as tar:
        tar.add(site_dir, arcname='site', filter=tar_filter)
        # Build scripts execute from ``site/`` but some shared release
        # contracts intentionally live at the repository root. Kaggle only
        # receives this archive, so include those exact, non-secret contracts
        # at their repository-relative paths instead of relying on the local
        # checkout.
        for relative_path in SITE_SOURCE_REPO_CONTRACTS:
            source_path = ROOT / relative_path
            if not source_path.is_file():
                raise FileNotFoundError(source_path)
            tar.add(source_path, arcname=relative_path.as_posix(), filter=tar_filter)


def prepare_site_source(args: argparse.Namespace, work_dir: Path) -> Path:
    staged_site = work_dir / 'site'
    copy_tree(SITE_SRC, staged_site)
    # The production related-chain audit must use the shared GoogleAIClient
    # (Supabase limiter + thought filtering), so the Kaggle site payload includes
    # the repo-local google_ai package instead of a direct provider shortcut.
    copy_tree(ROOT / 'google_ai', staged_site / 'google_ai')
    sync_script = ROOT / 'scripts' / 'sync_event_search_vectors_to_supabase.py'
    if sync_script.exists():
        shutil.copy2(sync_script, staged_site / 'scripts' / 'sync_event_search_vectors_to_supabase.py')
    unusual_fixture = ROOT / 'tests' / 'fixtures' / 'unusual_events_golden_v1.json'
    if unusual_fixture.exists():
        shutil.copy2(unusual_fixture, staged_site / 'scripts' / unusual_fixture.name)
    service_share_renderer = KERNEL_SRC / 'service_share_card.py'
    if service_share_renderer.exists():
        shutil.copy2(
            service_share_renderer,
            staged_site / 'scripts' / service_share_renderer.name,
        )
    collection_product_quality = ROOT / 'scripts' / 'check_static_collections_product_quality.py'
    if not collection_product_quality.is_file():
        raise FileNotFoundError(collection_product_quality)
    shutil.copy2(
        collection_product_quality,
        staged_site / 'scripts' / collection_product_quality.name,
    )
    if args.db and not args.export_in_kaggle:
        exporter = staged_site / 'scripts' / 'export-production-preview-data.py'
        cmd = [
            sys.executable,
            str(exporter),
            '--db', str(Path(args.db).resolve()),
            '--limit', str(args.limit),
            '--current-date', args.current_date,
            '--output-dir', str(staged_site / 'src' / 'data'),
            '--catalog-mode', args.catalog_mode,
        ]
        if args.current_datetime:
            cmd.extend(['--current-datetime', args.current_datetime])
        if args.focus_date_from:
            cmd.extend(['--focus-date-from', args.focus_date_from])
        if args.focus_date_to:
            cmd.extend(['--focus-date-to', args.focus_date_to])
        if args.related_cache:
            cmd.extend(['--related-cache', str(Path(args.related_cache).resolve())])
        cmd.extend([
            '--related-mode', args.related_mode,
            '--related-corpus-revision', args.related_corpus_revision,
            '--related-response-max-bytes', str(getattr(args, 'related_response_max_bytes', 256 * 1024)),
            '--related-total-response-max-bytes', str(getattr(args, 'related_total_response_max_bytes', 16 * 1024 * 1024)),
            '--pgvector-embedding-model', args.pgvector_embedding_model,
            '--pgvector-embedding-key-env', args.pgvector_embedding_key_env,
            '--pgvector-max-provider-calls', str(args.pgvector_max_provider_calls),
            '--site-origin', args.public_site_origin,
            '--base-path', args.build_id or '',
            '--ics-base-url', args.ics_base_url,
            '--bge-vector-cache', str(Path(getattr(args, 'bge_vector_cache', ARTIFACT_ROOT / 'static_event_bge_vectors.npz')).resolve()),
            '--bge-vector-receipt', str(Path(getattr(args, 'bge_vector_receipt', ARTIFACT_ROOT / 'static_event_bge_vectors.receipt.json')).resolve()),
            '--bge-model-revision', getattr(args, 'bge_model_revision', '5617a9f61b028005a4858fdac845db406aefb181'),
            '--bge-batch-size', str(getattr(args, 'bge_batch_size', 8)),
            '--unusual-cache', str(Path(getattr(args, 'unusual_cache', ARTIFACT_ROOT / 'unusual_events_cache.json')).resolve()),
            '--unusual-last-good', str(Path(getattr(args, 'unusual_last_good', ARTIFACT_ROOT / 'unusual_events_last_good.json')).resolve()),
        ])
        if collection_semantic_compute_required(args):
            cmd.extend([
                '--collection-semantic-compute',
                '--collection-batch-output',
                str(staged_site / 'src' / 'data' / 'collection-batch-v1.json'),
                '--collection-batch-last-good',
                str(Path(getattr(args, 'collection_batch_last_good', ARTIFACT_ROOT / 'collection-batch-last-good.json')).resolve()),
                '--collection-product-snapshot-output',
                str(staged_site / 'src' / 'data' / 'static-collection-product-snapshot-v1.json'),
                '--collection-product-source-scope',
                str(getattr(args, 'collection_product_source_scope', 'static-site-builder-export')),
                '--collection-product-evidence-trust-scope',
                str(getattr(args, 'collection_product_evidence_trust_scope', 'all')),
            ])
        if args.profile != 'preview':
            cmd.extend([
                '--repo-sha', args.repo_sha,
                '--run-id', args.run_id,
                '--build-id', args.build_id,
                '--snapshot-id', str(args.snapshot_contract['snapshot_id']),
                '--snapshot-sha256', str(args.snapshot_contract['sha256']),
                '--snapshot-size', str(args.snapshot_contract['size']),
            ])
        if args.sync_pgvector_vectors:
            cmd.append('--sync-pgvector-vectors')
        if args.gemma_related_verify:
            cmd.append('--gemma-related-verify')
        if getattr(args, 'unusual_enabled', False):
            cmd.append('--unusual-enabled')
        if getattr(args, 'unusual_migration', False):
            cmd.append('--unusual-migration')
        cmd.extend([
            '--gemma-related-model', args.gemma_related_model,
            '--gemma-related-key-env', args.gemma_related_key_env,
            '--gemma-related-max-anchors', str(args.gemma_related_max_anchors),
        ])
        run(cmd)
    return staged_site


def write_dataset_metadata(dataset_dir: Path, dataset_ref: str, title: str) -> None:
    (dataset_dir / 'dataset-metadata.json').write_text(json.dumps({
        'title': title,
        'id': dataset_ref,
        'licenses': [{'name': 'CC0-1.0'}],
    }, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')


def create_input_dataset(client, dataset_dir: Path, dataset_ref: str) -> None:
    # CherryFlash/VideoAnnounce pattern: create a unique dataset per run instead of
    # updating a fixed dataset. Fixed updates can race permissions/propagation and
    # make kernels_push reject dataset_sources.
    # Kaggle API 2.x keeps resumable-upload state in /tmp/.kaggle/uploads. In
    # long-lived local/Fly processes a stale state file can break a later dataset
    # upload with "KaggleObject.from_dict() got an unexpected keyword argument
    # 'token'" and then make the freshly-created private dataset invisible to
    # kernels_push. The cache is not credentials; it is safe to drop before a
    # one-shot generated dataset upload.
    shutil.rmtree(Path(tempfile.gettempdir()) / '.kaggle' / 'uploads', ignore_errors=True)
    last_error: Exception | None = None
    for attempt in range(1, 4):
        try:
            client.create_dataset(dataset_dir, public=False, quiet=True)
            print(f'[static-site-kaggle] dataset created: {dataset_ref}', flush=True)
            return
        except Exception as exc:
            last_error = exc
            delay = 10 * attempt
            detail = f'{exc}'
            if getattr(exc, '__cause__', None) is not None:
                detail += f' cause={exc.__cause__!r}'
            print(f'[static-site-kaggle] dataset create retry {attempt}/3 after {delay}s: {detail}', flush=True)
            time.sleep(delay)
    raise RuntimeError(f'Kaggle dataset create failed for {dataset_ref}: {last_error!r}')


def wait_dataset_ready(client, dataset_ref: str, *, expected_files: list[str], timeout_seconds: int = 180) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    last_status = ''
    last_files: list[str] = []
    while time.monotonic() < deadline:
        try:
            last_status = client.dataset_status(dataset_ref)
            files = client.dataset_list_files(dataset_ref, page_size=max(20, len(expected_files) + 5))
            last_files = [str(item.get('name') or '').strip() for item in files if str(item.get('name') or '').strip()]
            ready = str(last_status).strip().lower() == 'ready' and all(name in last_files for name in expected_files)
            print(f'[static-site-kaggle] dataset ready check status={last_status} files={last_files} ready={ready}', flush=True)
            if ready:
                return
        except Exception as exc:
            last_error = exc
            print(f'[static-site-kaggle] dataset ready check error: {exc}', flush=True)
        time.sleep(5)
    details = f'status={last_status} files={last_files} expected={expected_files}'
    if last_error is not None:
        details += f' last_error={last_error}'
    raise TimeoutError(f'Kaggle dataset did not become ready: {dataset_ref} {details}')


def wait_kernel_dataset_sources(client, kernel_ref: str, expected_sources: list[str], *, timeout_seconds: int = 180) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_meta: dict | None = None
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            matched, meta = client.kernel_has_dataset_sources(kernel_ref, expected_sources)
            last_meta = meta
            print(
                '[static-site-kaggle] kernel dataset source check '
                f"matched={matched} actual={meta.get('dataset_sources')}",
                flush=True,
            )
            if matched:
                return
        except Exception as exc:
            last_error = exc
            print(f'[static-site-kaggle] kernel dataset source check error: {exc}', flush=True)
        time.sleep(10)
    actual = (last_meta or {}).get('dataset_sources')
    details = f'expected={expected_sources} actual={actual}'
    if last_error is not None:
        details += f' last_error={last_error}'
    raise TimeoutError(f'Kaggle kernel did not bind dataset sources: {kernel_ref} {details}')


def slugify(value: str, *, max_len: int = 48) -> str:
    out = ''.join(ch.lower() if ch.isascii() and ch.isalnum() else '-' for ch in str(value or '').strip())
    out = '-'.join(part for part in out.split('-') if part)
    return (out or 'run')[:max_len].strip('-') or 'run'


def first_env(*names: str, default: str = '') -> str:
    for name in names:
        value = (os.getenv(name) or '').strip()
        if value:
            return value
    return default


def build_runtime_secret_payload(args: argparse.Namespace) -> dict[str, str]:
    needs_runtime_secrets = bool(args.gemma_related_verify or args.related_mode == 'pgvector' or args.sync_pgvector_vectors)
    if not needs_runtime_secrets:
        return {}
    key_env = (args.gemma_related_key_env or 'GOOGLE_API_KEY4').strip() or 'GOOGLE_API_KEY4'
    embedding_key_env = (args.pgvector_embedding_key_env or 'GOOGLE_API_KEY4').strip() or 'GOOGLE_API_KEY4'
    names = [
        key_env,
        embedding_key_env,
        'SUPABASE_URL',
        'SUPABASE_KEY',
        'SUPABASE_SERVICE_KEY',
        'SUPABASE_SERVICE_ROLE_KEY',
        'SUPABASE_SCHEMA',
        'PERSONALIZATION_SUPABASE_URL',
        'PERSONALIZATION_SUPABASE_SECRET_KEY',
        'PERSONALIZATION_SUPABASE_SERVICE_ROLE_KEY',
        'PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY',
        'GOOGLE_AI_LIMITER_SUPABASE_URL',
        'GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY',
        # These are release gates, not credentials, but the Kaggle exporter and
        # Astro build both need the exact values selected by the Fly runtime.
        'ENABLE_INTEREST_CLUB_STATIC_PROJECTION',
        'PUBLIC_INTEREST_CLUBS_ENABLED',
        'GOOGLE_API_LOCALNAME',
        'GOOGLE_AI_RESERVE_SCOPE_TO_DEFAULT_ENV',
        'GOOGLE_AI_RESERVE_DIRECT_RETRY',
        'GOOGLE_AI_RESERVE_DIRECT_SCHEMA',
    ]
    payload = {name: os.getenv(name, '').strip() for name in dict.fromkeys(names) if os.getenv(name, '').strip()}
    missing = []
    if args.gemma_related_verify and not payload.get(key_env):
        missing.append(key_env)
    if args.sync_pgvector_vectors and not payload.get(embedding_key_env):
        missing.append(embedding_key_env)
    if (args.gemma_related_verify or args.sync_pgvector_vectors) and not payload.get('GOOGLE_AI_LIMITER_SUPABASE_URL'):
        missing.append('GOOGLE_AI_LIMITER_SUPABASE_URL')
    if (
        args.gemma_related_verify or args.sync_pgvector_vectors
    ) and not payload.get('GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY'):
        missing.append('GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY')
    if args.gemma_related_verify and not payload.get('SUPABASE_URL'):
        missing.append('SUPABASE_URL')
    if args.gemma_related_verify and not any(payload.get(name) for name in ('SUPABASE_SERVICE_KEY', 'SUPABASE_KEY', 'SUPABASE_SERVICE_ROLE_KEY')):
        missing.append('SUPABASE_KEY/SUPABASE_SERVICE_KEY')
    if args.related_mode == 'pgvector' and not payload.get('PERSONALIZATION_SUPABASE_URL'):
        missing.append('PERSONALIZATION_SUPABASE_URL')
    if args.related_mode == 'pgvector' and not any(payload.get(name) for name in ('PERSONALIZATION_SUPABASE_SECRET_KEY', 'PERSONALIZATION_SUPABASE_SERVICE_ROLE_KEY')):
        missing.append('PERSONALIZATION_SUPABASE_SECRET_KEY')
    if missing:
        raise RuntimeError(
            'Missing envs for encrypted Kaggle Gemma limiter dataset: '
            + ', '.join(missing)
        )
    return payload


def encrypt_secret_payload(payload: dict[str, str]) -> tuple[bytes, bytes]:
    from cryptography.fernet import Fernet

    key = Fernet.generate_key()
    encrypted = Fernet(key).encrypt(json.dumps(payload, ensure_ascii=False).encode('utf-8'))
    return encrypted, key


def create_secret_datasets_if_needed(
    args: argparse.Namespace,
    *,
    client,
    env_user: str,
    build_id: str,
    tmp_root: Path,
) -> list[str]:
    payload = build_runtime_secret_payload(args)
    if not payload:
        return []
    encrypted, fernet_key = encrypt_secret_payload(payload)
    digest = hashlib.sha1(build_id.encode('utf-8')).hexdigest()[:8]
    run_suffix = f"{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{secrets.token_hex(3)}"
    # Kaggle dataset slugs have tight length/charset constraints. Keep secret
    # dataset slugs intentionally short; the build id is recoverable from the
    # input dataset and from the hash suffix.
    slug_base = f"{digest}-{run_suffix}"

    cipher_dir = tmp_root / 'secret-cipher'
    key_dir = tmp_root / 'secret-key'
    cipher_dir.mkdir(parents=True, exist_ok=True)
    key_dir.mkdir(parents=True, exist_ok=True)
    (cipher_dir / 'secrets.enc').write_bytes(encrypted)
    (cipher_dir / 'config.json').write_text(json.dumps({
        'schema_version': 1,
        'purpose': 'static-site-builder-runtime-secrets',
        'secret_env_names': sorted(payload.keys()),
        'generated_at': datetime.now(timezone.utc).isoformat(),
    }, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')
    (key_dir / 'fernet.key').write_bytes(fernet_key)
    # `fernet.keys` keeps compatibility with the shared google_ai.SecretsProvider;
    # `fernet.key` keeps compatibility with older repo notebooks.
    (key_dir / 'fernet.keys').write_text(fernet_key.decode('ascii') + '\n', encoding='utf-8')
    (key_dir / 'config.json').write_text(json.dumps({
        'schema_version': 1,
        'purpose': 'static-site-builder-runtime-secret-key',
        'generated_at': datetime.now(timezone.utc).isoformat(),
    }, ensure_ascii=False, indent=2) + '\n', encoding='utf-8')

    cipher_ref = f'{env_user}/ssb-secrets-{slug_base}'
    key_ref = f'{env_user}/ssb-key-{slug_base}'
    write_dataset_metadata(cipher_dir, cipher_ref, f'SSB secrets {run_suffix}')
    write_dataset_metadata(key_dir, key_ref, f'SSB key {run_suffix}')
    create_input_dataset(client, cipher_dir, cipher_ref)
    create_input_dataset(client, key_dir, key_ref)
    wait_dataset_ready(client, cipher_ref, expected_files=['secrets.enc', 'config.json'])
    wait_dataset_ready(client, key_ref, expected_files=['fernet.key', 'fernet.keys', 'config.json'])
    print(
        '[static-site-kaggle] encrypted secret datasets ready: '
        f"cipher={cipher_ref} key={key_ref} envs={sorted(payload.keys())}",
        flush=True,
    )
    return [cipher_ref, key_ref]


def cleanup_secret_datasets(client, dataset_refs: list[str]) -> None:
    for dataset_ref in dataset_refs:
        try:
            client.delete_dataset(dataset_ref, no_confirm=True)
            print(f'[static-site-kaggle] encrypted secret dataset deleted: {dataset_ref}', flush=True)
        except Exception as exc:
            print(f'[static-site-kaggle] encrypted secret dataset cleanup failed: {dataset_ref}: {exc}', flush=True)


def resolve_status_callback_url(args: argparse.Namespace) -> str | None:
    explicit = (args.status_callback_url or os.getenv('KAGGLE_STATUS_CALLBACK_URL') or '').strip()
    if explicit:
        return explicit
    webhook = (os.getenv('WEBHOOK_URL') or '').strip()
    if webhook:
        return webhook.rstrip('/') + '/internal/kaggle/run-event'
    return None


async def _create_status_config_and_close(db, create_run_config, **kwargs):
    """Build the callback config without leaking an aiosqlite worker thread."""

    try:
        return await create_run_config(db, **kwargs)
    finally:
        await db.close()


def create_status_dataset_if_configured(
    args: argparse.Namespace,
    client,
    *,
    env_user: str,
    build_id: str,
    kernel_ref: str,
    dataset_ref: str,
) -> str | None:
    status_db = (args.status_db or os.getenv('STATIC_SITE_STATUS_DB') or '').strip()
    callback_url = resolve_status_callback_url(args)
    if not status_db or not callback_url:
        print(
            '[static-site-kaggle] status dataset skipped: '
            f"status_db={'yes' if status_db else 'no'} callback_url={'yes' if callback_url else 'no'}",
            flush=True,
        )
        return None
    from db import Database
    from kaggle_status import create_kaggle_run_config, create_kaggle_status_dataset

    run_id = args.run_id or f'static-site-builder:{build_id}'
    db = Database(status_db)
    config = asyncio.run(
        _create_status_config_and_close(
            db,
            create_kaggle_run_config,
            run_id=run_id,
            session_id=None,
            kind='static_site_builder',
            notebook='StaticSiteBuilder',
            kernel_ref=kernel_ref,
            dataset_ref=dataset_ref,
            callback_url=callback_url,
            resource_leases=['static_site:builder'],
        )
    )
    if not config:
        return None
    status_dataset = create_kaggle_status_dataset(
        client,
        username=env_user,
        slug_prefix='status-static-site-builder',
        run_id=run_id,
        config=config,
    )
    if status_dataset:
        wait_dataset_ready(client, status_dataset, expected_files=['kaggle_run.json', 'kaggle_status_client.py'])
        print(f'[static-site-kaggle] status dataset ready: {status_dataset}', flush=True)
    return status_dataset


def stage_kernel_and_dataset(args: argparse.Namespace, staging: Path, dataset_dir: Path, client, env_user: str) -> tuple[str, str]:
    if not KERNEL_SRC.exists():
        raise FileNotFoundError(KERNEL_SRC)
    args.repo_sha = resolve_repo_sha(getattr(args, 'repo_sha', ''))
    copy_tree(KERNEL_SRC, staging)
    image_source = getattr(args, 'image_source_contract', None) or resolve_image_source_contract(args)
    build_id = args.build_id or f"preview-{datetime.now(timezone.utc).strftime('%Y%m%d-static-prod50')}"
    search_receipt_source: Path | None = None
    configured_search_receipt = str(getattr(args, 'search_corpus_receipt', '') or '').strip()
    search_receipt_required = (
        args.profile == 'production-candidate'
        and bool(args.secret_candidate_require_authorized_search)
    )
    if configured_search_receipt:
        search_receipt_source = Path(configured_search_receipt).resolve()
        validate_search_corpus_receipt(search_receipt_source)
    elif search_receipt_required:
        raise RuntimeError('authorized Search candidate requires the vector-owner corpus receipt')
    config = {
        'build_id': build_id,
        'run_id': args.run_id or build_id,
        'profile': args.profile,
        'catalog_mode': args.catalog_mode,
        'repo_sha': args.repo_sha or None,
        'candidate_token': args.candidate_token or None,
        'snapshot': args.snapshot_contract or None,
        'current_date': args.current_date,
        'current_datetime': args.current_datetime,
        'build_clock': args.build_clock,
        'input_fingerprint': args.input_fingerprint or None,
        'focus_date_from': args.focus_date_from or None,
        'focus_date_to': args.focus_date_to or None,
        'limit': args.limit,
        'public_site_origin': args.public_site_origin,
        'asset_base_url': args.asset_base_url or None,
        'astro_asset_base_url': resolve_build_template(args.astro_asset_base_url, build_id),
        'ics_base_url': args.ics_base_url or None,
        'public_personalization_supabase_url': args.public_personalization_supabase_url or None,
        'public_personalization_supabase_publishable_key': args.public_personalization_supabase_publishable_key or None,
        'public_personalization_supabase_relay_url': getattr(args, 'public_personalization_supabase_relay_url', '') or None,
        'public_yandex_auth_provider': args.public_yandex_auth_provider or 'custom:yandex',
        'public_authorized_search_transport': getattr(args, 'public_authorized_search_transport', '') or 'json',
        'secret_candidate_artifact_research': bool(args.secret_candidate_artifact_research),
        'secret_candidate_require_authorized_search': bool(args.secret_candidate_require_authorized_search),
        'search_corpus_receipt_filename': (
            'event-search-corpus-receipt.json' if search_receipt_source else None
        ),
        'export_in_kaggle': bool(args.export_in_kaggle),
        'sqlite_db_filename': 'events.sqlite' if args.db and args.export_in_kaggle else None,
        'related_cache_filename': 'event_related_chain_cache.json',
        'bge_vector_cache_filename': 'static_event_bge_vectors.npz',
        'bge_vector_receipt_filename': 'static_event_bge_vectors.receipt.json',
        'unusual_cache_filename': 'unusual_events_cache.json',
        'unusual_last_good_filename': 'unusual_events_last_good.json',
        'collection_batch_filename': 'collection-batch-v1.json',
        'collection_batch_last_good_filename': 'collection-batch-last-good.json',
        'collection_product_snapshot_filename': 'static-collection-product-snapshot-v1.json',
        'collection_product_source_scope': str(getattr(args, 'collection_product_source_scope', 'static-site-builder-export')),
        'collection_product_evidence_trust_scope': str(getattr(args, 'collection_product_evidence_trust_scope', 'all')),
        'related_mode': args.related_mode,
        'related_corpus_revision': args.related_corpus_revision or None,
        'related_response_max_bytes': getattr(args, 'related_response_max_bytes', 256 * 1024),
        'related_total_response_max_bytes': getattr(args, 'related_total_response_max_bytes', 16 * 1024 * 1024),
        'sync_pgvector_vectors': bool(args.sync_pgvector_vectors),
        'pgvector_embedding_model': args.pgvector_embedding_model,
        'pgvector_embedding_key_env': args.pgvector_embedding_key_env,
        'pgvector_max_provider_calls': args.pgvector_max_provider_calls,
        'gemma_related_verify': bool(args.gemma_related_verify),
        'gemma_related_model': args.gemma_related_model,
        'gemma_related_key_env': args.gemma_related_key_env,
        'gemma_related_max_anchors': args.gemma_related_max_anchors,
        'bge_model_revision': getattr(args, 'bge_model_revision', '5617a9f61b028005a4858fdac845db406aefb181'),
        'bge_batch_size': getattr(args, 'bge_batch_size', 8),
        'unusual_enabled': bool(getattr(args, 'unusual_enabled', False)),
        'unusual_migration': bool(getattr(args, 'unusual_migration', False)),
        'collection_semantic_compute': collection_semantic_compute_required(args),
        'queued_at': datetime.now(timezone.utc).isoformat(),
        'payload_mode': 'dataset_source',
    }
    staged_site = prepare_site_source(args, dataset_dir)
    if args.db and args.export_in_kaggle:
        shutil.copy2(Path(args.db).resolve(), dataset_dir / 'events.sqlite')
        if args.snapshot_manifest:
            shutil.copy2(Path(args.snapshot_manifest).resolve(), dataset_dir / 'snapshot-manifest.json')
    if args.related_cache and Path(args.related_cache).exists():
        shutil.copy2(Path(args.related_cache).resolve(), dataset_dir / 'event_related_chain_cache.json')
    if search_receipt_source:
        shutil.copy2(search_receipt_source, dataset_dir / 'event-search-corpus-receipt.json')
    semantic_inputs = (
        (
            (getattr(args, 'bge_vector_cache', ''), 'static_event_bge_vectors.npz'),
            (getattr(args, 'bge_vector_receipt', ''), 'static_event_bge_vectors.receipt.json'),
            (getattr(args, 'unusual_cache', ''), 'unusual_events_cache.json'),
            (getattr(args, 'unusual_last_good', ''), 'unusual_events_last_good.json'),
            (getattr(args, 'collection_batch_last_good', ''), 'collection-batch-last-good.json'),
        )
        if (
            collection_semantic_compute_required(args)
            or args.related_mode == 'bge'
            or getattr(args, 'unusual_enabled', False)
        )
        else ()
    )
    for source_value, filename in semantic_inputs:
        if source_value and Path(source_value).is_file():
            shutil.copy2(Path(source_value).resolve(), dataset_dir / filename)
    # Deliberately avoid `.tar.gz` filename: Kaggle dataset ingestion auto-extracts
    # archives and may reject/disappear datasets containing Astro dynamic route
    # paths like `[slug].astro`. The file content is gzip tar; only the extension is
    # neutral.
    source_archive = dataset_dir / 'site_source.tarball'
    payload_tree_sha256 = payload_source_tree_digest(staged_site)
    tar_site_source(staged_site, source_archive)
    source_identity = {
        'schema_version': STATIC_SITE_SOURCE_IDENTITY_SCHEMA,
        'repo_sha': args.repo_sha,
        'image_source_manifest_sha256': image_source['manifest_sha256'],
        'image_source_tree_sha256': image_source['source_tree_sha256'],
        'payload_tree_sha256': payload_tree_sha256,
        'payload_archive_sha256': sha256_file(source_archive),
    }
    validate_static_site_source_identity(source_identity, repo_sha=args.repo_sha)
    source_manifest_bytes = _manifest_bytes(source_identity)
    (dataset_dir / 'static_site_source_manifest.json').write_bytes(
        source_manifest_bytes
    )
    config['source_manifest_sha256'] = hashlib.sha256(
        source_manifest_bytes
    ).hexdigest()
    (dataset_dir / 'build_config.json').write_text(
        json.dumps(config, ensure_ascii=False, indent=2) + '\n', encoding='utf-8'
    )
    shutil.rmtree(staged_site)
    run_suffix = f"{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{secrets.token_hex(3)}"
    dataset_ref = f'{env_user}/static-site-builder-input-{run_suffix}'
    write_dataset_metadata(dataset_dir, dataset_ref, f'static site builder input {run_suffix}')
    create_input_dataset(client, dataset_dir, dataset_ref)
    expected = [
        'site_source.tarball',
        'build_config.json',
        'static_site_source_manifest.json',
    ]
    if args.db and args.export_in_kaggle:
        expected.append('events.sqlite')
        if args.snapshot_manifest:
            expected.append('snapshot-manifest.json')
    if args.related_cache and Path(args.related_cache).exists():
        expected.append('event_related_chain_cache.json')
    if search_receipt_source:
        expected.append('event-search-corpus-receipt.json')
    expected.extend(
        filename
        for source_value, filename in semantic_inputs
        if source_value and Path(source_value).is_file()
    )
    wait_dataset_ready(client, dataset_ref, expected_files=expected)
    return build_id, dataset_ref

def embed_site_payload(staging: Path, site_dir: Path, config: dict) -> None:
    archive_path = staging / 'site_source.tarball'
    tar_site_source(site_dir, archive_path)
    encoded = base64.b64encode(archive_path.read_bytes()).decode('ascii')
    script_path = staging / 'static_site_builder.py'
    source = script_path.read_text(encoding='utf-8')
    source = source.replace("EMBEDDED_SITE_SOURCE_B64 = ''", f"EMBEDDED_SITE_SOURCE_B64 = {encoded!r}")
    source = source.replace("EMBEDDED_BUILD_CONFIG_JSON = ''", f"EMBEDDED_BUILD_CONFIG_JSON = {json.dumps(config, ensure_ascii=False)!r}")
    script_path.write_text(source, encoding='utf-8')
    archive_path.unlink(missing_ok=True)


def adopt_existing_kernel_output(args: argparse.Namespace, client, kernel_ref: str) -> int:
    """Reconcile one exact already-pushed kernel without creating a new run."""

    expected_dataset = str(args.expected_dataset_ref or '').strip()
    if not expected_dataset:
        print('[static-site-kaggle] adoption unavailable: durable dataset identity missing', flush=True)
        return ADOPT_REMOTE_UNAVAILABLE_EXIT
    try:
        matched, metadata = client.kernel_has_dataset_sources(kernel_ref, [expected_dataset])
    except Exception as exc:
        print(f'[static-site-kaggle] adoption dataset probe failed: {exc}', flush=True)
        return ADOPT_REMOTE_LIVE_EXIT
    if not matched:
        print(
            '[static-site-kaggle] adoption unavailable: fixed kernel now has different inputs '
            f"expected={expected_dataset} actual={metadata.get('dataset_sources')}",
            flush=True,
        )
        return ADOPT_REMOTE_UNAVAILABLE_EXIT
    status = client.get_kernel_status(kernel_ref)
    raw = str(status.get('status') or '').upper()
    print(f'[static-site-kaggle] adoption status={raw} raw={status}', flush=True)
    if raw in {'QUEUED', 'RUNNING', 'INITIALIZING', 'PREPARING'}:
        return ADOPT_REMOTE_LIVE_EXIT
    if raw != 'COMPLETE':
        return ADOPT_REMOTE_UNAVAILABLE_EXIT
    out_dir = prepare_output_directory(ARTIFACT_ROOT, args.build_id)
    # Replacing the local duplicate can itself restore enough durable space
    # for the exact remote output. Check capacity only after the stale partial
    # directory is removed; the remote Kaggle result remains authoritative and
    # can be retried if this post-cleanup probe still fails.
    require_static_site_storage_ready()
    files = client.download_kernel_output(kernel_ref, path=out_dir, force=True)
    print(f'[static-site-kaggle] adopted and downloaded {len(files)} files to {out_dir}', flush=True)
    validated = validate_downloaded_result(out_dir, args)
    if getattr(args, 'export_in_kaggle', False) and (
        collection_semantic_compute_required(args)
        or getattr(args, 'related_mode', 'sparse') == 'bge'
        or getattr(args, 'unusual_enabled', False)
    ):
        persist_semantic_outputs(out_dir, args, validated)
    print(
        '[static-site-kaggle] adopted exact output '
        f"build_id={validated.get('build_id')} result_sha256={sha256_file(out_dir / 'static_site_build_result.json')}",
        flush=True,
    )
    secret_sources = [
        str(item) for item in metadata.get('dataset_sources') or []
        if '/ssb-secrets-' in str(item) or '/ssb-key-' in str(item)
    ]
    if secret_sources and not args.keep_secret_datasets:
        cleanup_secret_datasets(client, secret_sources)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--db', help='Optional SQLite snapshot; if set, export preview JSON before staging')
    parser.add_argument('--snapshot-manifest', default=os.getenv('STATIC_SITE_SNAPSHOT_MANIFEST', ''))
    parser.add_argument('--profile', choices=['preview', 'production-candidate'], default=os.getenv('STATIC_SITE_BUILD_PROFILE', 'preview'))
    parser.add_argument('--catalog-mode', choices=['slice', 'full'], default=os.getenv('STATIC_SITE_CATALOG_MODE', 'slice'))
    parser.add_argument('--repo-sha', default=os.getenv('STATIC_SITE_REPO_SHA', ''))
    parser.add_argument(
        '--image-source-manifest',
        default=os.getenv('STATIC_SITE_IMAGE_SOURCE_MANIFEST_FILE', ''),
        help='Build-time manifest binding the deployed image revision to exact static source bytes.',
    )
    parser.add_argument(
        '--expected-image-source-manifest-sha256',
        default='',
        help='Durable manifest digest required when adopting an existing remote run.',
    )
    parser.add_argument('--run-id', default=os.getenv('STATIC_SITE_RUN_ID', ''))
    parser.add_argument('--candidate-token', default=os.getenv('STATIC_SITE_CANDIDATE_TOKEN', ''))
    parser.add_argument('--limit', type=int, default=int(os.getenv('STATIC_SITE_BUILDER_LIMIT', '50')))
    parser.add_argument('--current-date', default=os.getenv('STATIC_SITE_CURRENT_DATE', ''))
    parser.add_argument('--current-datetime', default=os.getenv('STATIC_SITE_CURRENT_DATETIME', ''))
    parser.add_argument('--input-fingerprint', default=os.getenv('STATIC_SITE_INPUT_FINGERPRINT', ''))
    parser.add_argument('--focus-date-from', default=os.getenv('STATIC_SITE_FOCUS_DATE_FROM', ''))
    parser.add_argument('--focus-date-to', default=os.getenv('STATIC_SITE_FOCUS_DATE_TO', ''))
    parser.add_argument('--build-id', default=os.getenv('STATIC_SITE_BUILD_ID'))
    parser.add_argument('--public-site-origin', default=os.getenv('PUBLIC_SITE_ORIGIN', 'https://kenigevents.ru'))
    parser.add_argument('--asset-base-url', default=os.getenv('PUBLIC_ASSET_BASE_URL', ''))
    parser.add_argument('--astro-asset-base-url', default=os.getenv('PUBLIC_ASTRO_ASSET_BASE_URL', ''))
    parser.add_argument('--ics-base-url', default=os.getenv('PUBLIC_ICS_BASE_URL', ''))
    parser.add_argument(
        '--public-personalization-supabase-url',
        default=first_env(
            'STATIC_SITE_PUBLIC_PERSONALIZATION_SUPABASE_URL',
            'PUBLIC_PERSONALIZATION_SUPABASE_URL',
            'PERSONALIZATION_SUPABASE_URL',
        ),
        help='Browser-safe Supabase project URL for AuthorizedEventSearch in the static build.',
    )
    parser.add_argument(
        '--public-personalization-supabase-publishable-key',
        default=first_env(
            'STATIC_SITE_PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY',
            'PUBLIC_PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY',
            'PERSONALIZATION_SUPABASE_PUBLISHABLE_KEY',
        ),
        help='Browser-safe Supabase publishable key for AuthorizedEventSearch in the static build.',
    )
    parser.add_argument(
        '--public-personalization-supabase-relay-url',
        default=first_env(
            'STATIC_SITE_PUBLIC_PERSONALIZATION_SUPABASE_RELAY_URL',
            'PUBLIC_PERSONALIZATION_SUPABASE_RELAY_URL',
            'PERSONALIZATION_SUPABASE_RELAY_URL',
        ),
        help='Browser-safe stateless relay URL used only by resilient static clients.',
    )
    parser.add_argument(
        '--public-yandex-auth-provider',
        default=first_env('STATIC_SITE_PUBLIC_YANDEX_AUTH_PROVIDER', 'PUBLIC_YANDEX_AUTH_PROVIDER', default='custom:yandex'),
        help='Supabase Auth provider id for Yandex OAuth in AuthorizedEventSearch.',
    )
    parser.add_argument(
        '--public-authorized-search-transport',
        choices=['json', 'ndjson'],
        default=first_env(
            'STATIC_SITE_PUBLIC_AUTHORIZED_SEARCH_TRANSPORT',
            'PUBLIC_AUTHORIZED_SEARCH_TRANSPORT',
            default='json',
        ),
        help='Browser transport for AuthorizedEventSearch; JSON is the mobile-safe default.',
    )
    parser.add_argument(
        '--secret-candidate-artifact-research',
        action='store_true',
        default=(os.getenv('STATIC_SITE_SECRET_CANDIDATE_ARTIFACT_RESEARCH', '').strip().lower() in {'1', 'true', 'yes', 'on'}),
        help='Enable the gated amber-artifact research only in the immutable secret candidate.',
    )
    parser.add_argument(
        '--secret-candidate-require-authorized-search',
        action='store_true',
        default=(os.getenv('STATIC_SITE_SECRET_CANDIDATE_REQUIRE_AUTHORIZED_SEARCH', '').strip().lower() in {'1', 'true', 'yes', 'on'}),
        help='Fail the secret-candidate build when public Search/Auth configuration is absent.',
    )
    parser.add_argument(
        '--search-corpus-receipt',
        default=os.getenv('STATIC_SITE_SEARCH_CORPUS_RECEIPT', ''),
        help='Complete v2 receipt produced by the dedicated Fly vector projection owner.',
    )
    parser.add_argument('--export-in-kaggle', action='store_true', default=(os.getenv('STATIC_SITE_EXPORT_IN_KAGGLE', '').strip().lower() in {'1', 'true', 'yes', 'on'}))
    parser.add_argument('--related-cache', default=os.getenv('STATIC_SITE_RELATED_CACHE', str(ARTIFACT_ROOT / 'event_related_chain_cache.json')))
    parser.add_argument('--related-mode', choices=['sparse', 'pgvector', 'bge'], default=os.getenv('STATIC_SITE_RELATED_MODE', 'sparse'))
    parser.add_argument('--related-corpus-revision', default=os.getenv('STATIC_SITE_RELATED_CORPUS_REVISION', ''))
    parser.add_argument('--related-response-max-bytes', type=int, default=int(os.getenv('STATIC_SITE_RELATED_RESPONSE_MAX_BYTES', str(256 * 1024)) or str(256 * 1024)))
    parser.add_argument('--related-total-response-max-bytes', type=int, default=int(os.getenv('STATIC_SITE_RELATED_TOTAL_RESPONSE_MAX_BYTES', str(16 * 1024 * 1024)) or str(16 * 1024 * 1024)))
    parser.add_argument('--sync-pgvector-vectors', action='store_true', default=(os.getenv('STATIC_SITE_SYNC_PGVECTOR_VECTORS', '').strip().lower() in {'1', 'true', 'yes', 'on'}))
    parser.add_argument('--pgvector-embedding-model', default=os.getenv('STATIC_SITE_PGVECTOR_EMBEDDING_MODEL', 'gemini-embedding-2'))
    parser.add_argument('--pgvector-embedding-key-env', default=os.getenv('STATIC_SITE_PGVECTOR_EMBEDDING_KEY_ENV', 'GOOGLE_API_KEY4'))
    parser.add_argument('--pgvector-max-provider-calls', type=int, default=int(os.getenv('STATIC_SITE_PGVECTOR_MAX_PROVIDER_CALLS', '1000') or '1000'))
    parser.add_argument('--gemma-related-verify', action='store_true', default=(os.getenv('STATIC_SITE_GEMMA_RELATED_VERIFY', '').strip().lower() in {'1', 'true', 'yes', 'on'}))
    parser.add_argument('--gemma-related-model', default=os.getenv('STATIC_SITE_GEMMA_RELATED_MODEL', 'models/gemma-4-26b-a4b-it'))
    parser.add_argument('--gemma-related-key-env', default=os.getenv('STATIC_SITE_GEMMA_RELATED_KEY_ENV', 'GOOGLE_API_KEY4'))
    parser.add_argument('--gemma-related-max-anchors', type=int, default=int(os.getenv('STATIC_SITE_GEMMA_RELATED_MAX_ANCHORS', '0') or '0'))
    parser.add_argument('--bge-vector-cache', default=os.getenv('STATIC_SITE_BGE_VECTOR_CACHE', str(ARTIFACT_ROOT / 'static_event_bge_vectors.npz')))
    parser.add_argument('--bge-vector-receipt', default=os.getenv('STATIC_SITE_BGE_VECTOR_RECEIPT', str(ARTIFACT_ROOT / 'static_event_bge_vectors.receipt.json')))
    parser.add_argument('--bge-model-revision', default=os.getenv('STATIC_SITE_BGE_MODEL_REVISION', '5617a9f61b028005a4858fdac845db406aefb181'))
    parser.add_argument('--bge-batch-size', type=int, default=int(os.getenv('STATIC_SITE_BGE_BATCH_SIZE', '8') or '8'))
    parser.add_argument('--unusual-cache', default=os.getenv('STATIC_SITE_UNUSUAL_CACHE', str(ARTIFACT_ROOT / 'unusual_events_cache.json')))
    parser.add_argument('--unusual-last-good', default=os.getenv('STATIC_SITE_UNUSUAL_LAST_GOOD', str(ARTIFACT_ROOT / 'unusual_events_last_good.json')))
    parser.add_argument('--collection-batch', default=os.getenv('STATIC_SITE_COLLECTION_BATCH', str(ARTIFACT_ROOT / 'collection-batch-v1.json')))
    parser.add_argument('--collection-batch-last-good', default=os.getenv('STATIC_SITE_COLLECTION_LAST_GOOD', str(ARTIFACT_ROOT / 'collection-batch-last-good.json')))
    parser.add_argument('--collection-product-snapshot', default=os.getenv('STATIC_SITE_COLLECTION_PRODUCT_SNAPSHOT', str(ARTIFACT_ROOT / 'static-collection-product-snapshot-v1.json')))
    parser.add_argument('--collection-product-source-scope', default=os.getenv('STATIC_SITE_COLLECTION_PRODUCT_SOURCE_SCOPE', 'static-site-builder-export'), help='Provenance label for the exact source DB snapshot/stage')
    parser.add_argument('--collection-product-evidence-trust-scope', choices=['all', 'trusted'], default=os.getenv('STATIC_SITE_COLLECTION_PRODUCT_EVIDENCE_TRUST_SCOPE', 'all'))
    parser.add_argument('--collection-semantic-compute', action='store_true', default=(os.getenv('STATIC_SITE_COLLECTION_SEMANTIC_COMPUTE', '').strip().lower() in {'1', 'true', 'yes', 'on'}))
    parser.add_argument('--unusual-enabled', action='store_true', default=(os.getenv('STATIC_SITE_UNUSUAL_ENABLED', '').strip().lower() in {'1', 'true', 'yes', 'on'}))
    parser.add_argument('--unusual-migration', action='store_true', default=(os.getenv('STATIC_SITE_UNUSUAL_MIGRATION', '1').strip().lower() in {'1', 'true', 'yes', 'on'}))
    parser.add_argument('--keep-secret-datasets', action='store_true', default=(os.getenv('STATIC_SITE_KEEP_SECRET_DATASETS', '').strip().lower() in {'1', 'true', 'yes', 'on'}))
    parser.add_argument('--timeout-minutes', type=int, default=int(os.getenv('STATIC_SITE_KAGGLE_TIMEOUT_MINUTES', '45')))
    parser.add_argument('--poll-interval', type=int, default=int(os.getenv('STATIC_SITE_KAGGLE_POLL_INTERVAL', '30')))
    parser.add_argument('--status-db', default=os.getenv('STATIC_SITE_STATUS_DB', ''), help='SQLite DB that owns kaggle_run_ledger for callback validation')
    parser.add_argument('--status-callback-url', default=os.getenv('KAGGLE_STATUS_CALLBACK_URL', ''), help='Override callback URL for kaggle status events')
    parser.add_argument('--no-wait', action='store_true')
    parser.add_argument('--download-output', action='store_true')
    parser.add_argument('--adopt-existing', action='store_true', help='Download/reconcile the exact already-pushed kernel; never push')
    parser.add_argument('--expected-dataset-ref', default='', help='Durable input dataset identity required for adoption')
    parser.add_argument('--keep-staging', action='store_true')
    args = parser.parse_args()
    args.repo_sha = resolve_repo_sha(args.repo_sha)
    clock = resolve_build_clock(
        current_date=args.current_date or None,
        current_datetime=args.current_datetime or None,
    )
    args.current_date = clock.effective_date
    args.current_datetime = clock.current_datetime
    args.build_clock = asdict(clock)
    args.build_id = args.build_id or f"preview-{datetime.now(timezone.utc).strftime('%Y%m%d-static-prod50')}"
    if not BUILD_ID_RE.fullmatch(args.build_id):
        raise SystemExit('--build-id must be one bounded preview-* or production-* identity')
    if (
        args.related_mode == 'bge'
        or args.unusual_enabled
        or args.collection_semantic_compute
        or args.profile == 'production-candidate'
    ) and not re.fullmatch(
        r'[0-9a-f]{40}', args.bge_model_revision or ''
    ):
        raise SystemExit('shared BGE requires a pinned 40-character --bge-model-revision')
    if args.related_mode != 'pgvector' and args.sync_pgvector_vectors:
        raise SystemExit(
            '--sync-pgvector-vectors is restricted to pgvector mode; '
            'shared BGE builds keep provider_calls=0'
        )
    if args.related_response_max_bytes < 1024:
        raise SystemExit('--related-response-max-bytes must be at least 1024')
    if args.related_total_response_max_bytes < args.related_response_max_bytes:
        raise SystemExit('--related-total-response-max-bytes must be at least the per-response limit')

    if args.profile == 'production-candidate':
        # A production candidate may keep every new route shadow/blocked, but
        # it may not skip semantic computation or receipt validation.
        args.collection_semantic_compute = True
        if args.catalog_mode != 'full' or not args.export_in_kaggle:
            raise SystemExit('production-candidate requires --catalog-mode full and --export-in-kaggle')
        if not re.fullmatch(r'[0-9a-f]{40}', args.repo_sha or ''):
            raise SystemExit('production-candidate requires --repo-sha with a full 40-character SHA')
        if not args.run_id:
            raise SystemExit('production-candidate requires --run-id')
        if not re.fullmatch(r'[0-9a-f]{64}', args.input_fingerprint or ''):
            raise SystemExit('production-candidate requires --input-fingerprint SHA-256')
        if not re.fullmatch(r'production-[A-Za-z0-9][A-Za-z0-9._-]{0,191}', args.build_id):
            raise SystemExit('production-candidate requires a production-* --build-id')
        if args.candidate_token and not re.fullmatch(r'[A-Za-z0-9_-]{43}', args.candidate_token):
            raise SystemExit('--candidate-token must be one 256-bit base64url value')
        args.candidate_token = args.candidate_token or secrets.token_urlsafe(32)
    elif args.catalog_mode != 'slice':
        raise SystemExit('preview profile requires --catalog-mode slice')
    args.snapshot_contract = load_snapshot_contract(args)
    args.image_source_contract = resolve_image_source_contract(args)

    ARTIFACT_ROOT.mkdir(parents=True, exist_ok=True)
    with LOCK_PATH.open('w') as lock_file:
        try:
            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit('static-site Kaggle builder is already running locally')
        scratch_prune = prune_abandoned_static_site_scratch(SCRATCH_ROOT)
        if scratch_prune['removed_directories']:
            print(
                '[static-site-kaggle] abandoned scratch cleanup '
                f"directories={scratch_prune['removed_directories']} "
                f"bytes={scratch_prune['removed_bytes']}",
                flush=True,
            )
        # Import after --help parsing so optional Kaggle deps are not required for docs.
        from video_announce.kaggle_client import KaggleClient

        env_user = (os.getenv('KAGGLE_USERNAME') or '').strip()
        if not env_user:
            raise RuntimeError('KAGGLE_USERNAME is required')
        client = KaggleClient()
        kernel_ref = f'{env_user}/kenigevents-static-site-builder'
        if args.adopt_existing:
            return adopt_existing_kernel_output(args, client, kernel_ref)
        require_static_site_storage_ready()

        with tempfile.TemporaryDirectory(
            prefix='static-site-kaggle-', dir=SCRATCH_ROOT
        ) as tmp:
            tmp_root = Path(tmp)
            staging = tmp_root / 'kernel'
            dataset_dir = tmp_root / 'dataset'
            staging.mkdir(parents=True)
            dataset_dir.mkdir(parents=True)

            build_id, dataset_ref = stage_kernel_and_dataset(args, staging, dataset_dir, client, env_user)
            secret_dataset_refs = create_secret_datasets_if_needed(
                args,
                client=client,
                env_user=env_user,
                build_id=build_id,
                tmp_root=tmp_root,
            )
            if args.keep_staging:
                keep = ARTIFACT_ROOT / f'staging-{build_id}'
                if keep.exists():
                    shutil.rmtree(keep)
                shutil.copytree(tmp_root, keep)
                print(f"[static-site-kaggle] staging kept: {keep}", flush=True)

            try:
                dataset_sources = [dataset_ref, *secret_dataset_refs]
                status_dataset = create_status_dataset_if_configured(
                    args,
                    client,
                    env_user=env_user,
                    build_id=build_id,
                    kernel_ref=kernel_ref,
                    dataset_ref=dataset_ref,
                )
                if status_dataset:
                    dataset_sources.append(status_dataset)
                client.push_kernel(kernel_path=staging, dataset_sources=dataset_sources)
                print(f"[static-site-kaggle] pushed {kernel_ref} build_id={build_id} datasets={dataset_sources}", flush=True)
                wait_kernel_dataset_sources(client, kernel_ref, dataset_sources)
                if args.no_wait:
                    return 0
                deadline = time.monotonic() + max(60, args.timeout_minutes * 60)
                last_status = None
                while time.monotonic() < deadline:
                    time.sleep(max(5, args.poll_interval))
                    status = client.get_kernel_status(kernel_ref)
                    last_status = status
                    raw = str(status.get('status') or '').upper()
                    print(f"[static-site-kaggle] status={raw} raw={status}", flush=True)
                    if raw == 'COMPLETE':
                        if args.download_output:
                            out_dir = prepare_output_directory(ARTIFACT_ROOT, build_id)
                            files = client.download_kernel_output(kernel_ref, path=out_dir, force=True)
                            print(f"[static-site-kaggle] downloaded {len(files)} files to {out_dir}", flush=True)
                            validated = validate_downloaded_result(out_dir, args)
                            print(
                                '[static-site-kaggle] checked output '
                                f"build_id={validated.get('build_id')} result_sha256={sha256_file(out_dir / 'static_site_build_result.json')}",
                                flush=True,
                            )
                            cache_out = out_dir / 'event_related_chain_cache.json'
                            if args.export_in_kaggle and args.related_cache and cache_out.exists():
                                cache_target = Path(args.related_cache)
                                cache_target.parent.mkdir(parents=True, exist_ok=True)
                                atomic_copy_file(cache_out, cache_target)
                                print(f"[static-site-kaggle] related cache persisted atomically: {cache_target}", flush=True)
                            if args.export_in_kaggle and (
                                collection_semantic_compute_required(args)
                                or args.related_mode == 'bge'
                                or getattr(args, 'unusual_enabled', False)
                            ):
                                persist_semantic_outputs(out_dir, args, validated)
                        return 0
                    if raw in {'ERROR', 'FAILED', 'CANCELLED'}:
                        raise RuntimeError(f'Kaggle static-site builder failed: {status}')
                raise TimeoutError(f'Kaggle static-site builder timeout; last_status={last_status}')
            finally:
                if secret_dataset_refs and not args.no_wait and not args.keep_secret_datasets:
                    cleanup_secret_datasets(client, secret_dataset_refs)


if __name__ == '__main__':
    raise SystemExit(main())
