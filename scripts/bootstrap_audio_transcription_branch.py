from __future__ import annotations

import base64
import hashlib
import shutil
import tarfile
import tempfile
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
BOOTSTRAP = REPO / ".audio-transcription-bootstrap"
ARCHIVE_SHA256 = "71a9cf1cd74b1938db23e52f9ac5a3c301ecf8bd3c4f2fd526912711e90b473e"


def _replace_once(path: Path, old: str, new: str) -> None:
    text = path.read_text(encoding="utf-8")
    if new in text:
        return
    if old not in text:
        raise RuntimeError(f"expected patch anchor not found in {path}")
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


def _safe_extract(archive: Path, destination: Path) -> None:
    with tarfile.open(archive, mode="r:xz") as bundle:
        for member in bundle.getmembers():
            member_path = Path(member.name)
            if member_path.is_absolute() or ".." in member_path.parts:
                raise RuntimeError(f"unsafe archive path: {member.name}")
            if member.issym() or member.islnk():
                raise RuntimeError(f"links are forbidden in bootstrap archive: {member.name}")
        bundle.extractall(destination, filter="data")


def _copy_tree(source: Path, destination: Path) -> None:
    if not source.is_dir():
        raise RuntimeError(f"missing bootstrap directory: {source}")
    shutil.copytree(source, destination, dirs_exist_ok=True)


def _append_once(path: Path, marker: str, text: str) -> None:
    current = path.read_text(encoding="utf-8")
    if marker in current:
        return
    path.write_text(current.rstrip() + "\n\n" + text.strip() + "\n", encoding="utf-8")


def main() -> None:
    encoded_parts = sorted(BOOTSTRAP.glob("audio-part-*.b64"))
    if not encoded_parts:
        raise RuntimeError("audio bootstrap payload is missing")
    encoded = "".join(part.read_text(encoding="ascii").strip() for part in encoded_parts)
    raw = base64.b64decode(encoded, validate=True)
    observed = hashlib.sha256(raw).hexdigest()
    if observed != ARCHIVE_SHA256:
        raise RuntimeError(f"bootstrap archive digest mismatch: {observed}")

    with tempfile.TemporaryDirectory(prefix="audio-transcription-bootstrap-") as temporary:
        temp = Path(temporary)
        archive = temp / "audio-skeleton.tar.xz"
        archive.write_bytes(raw)
        extracted = temp / "extracted"
        extracted.mkdir()
        _safe_extract(archive, extracted)

        _copy_tree(extracted / "audio_transcription", REPO / "audio_transcription")
        _copy_tree(
            extracted / "kaggle" / "AudioTranscription",
            REPO / "kaggle" / "AudioTranscription",
        )
        _copy_tree(
            extracted / "docs" / "features" / "audio-transcription",
            REPO / "docs" / "features" / "audio-transcription",
        )
        for test in sorted((extracted / "tests").glob("test_audio_transcription_*.py")):
            shutil.copy2(test, REPO / "tests" / test.name)
        shutil.copy2(
            extracted / "scripts" / "validate_audio_transcription_skeleton.py",
            REPO / "scripts" / "validate_audio_transcription_skeleton.py",
        )
        shutil.copy2(
            extracted / "private_events_mcp_access_policy.py",
            REPO / "private_events_mcp" / "access_policy.py",
        )
        shutil.copy2(
            extracted / "private_events_mcp_integration.py",
            REPO / "private_events_mcp" / "integration.py",
        )

    guard = REPO / "remote_telegram_session.py"
    _replace_once(
        guard,
        'REMOTE_TELEGRAM_KAGGLE_JOB_TYPES = frozenset(\n    {\n',
        'REMOTE_TELEGRAM_KAGGLE_JOB_TYPES = frozenset(\n    {\n        "audio_transcription",\n',
    )

    guard_test = REPO / "tests" / "test_remote_telegram_session.py"
    _append_once(
        guard_test,
        "def test_audio_transcription_remote_job_type_is_guarded():",
        '''
def test_audio_transcription_remote_job_type_is_guarded():
    assert "audio_transcription" in guard.REMOTE_TELEGRAM_KAGGLE_JOB_TYPES
''',
    )

    env_file = REPO / ".env.example"
    _append_once(
        env_file,
        "# Telegram-native audio transcription for Private Events MCP",
        '''
# Telegram-native audio transcription for Private Events MCP (default off).
# Uses one dedicated Premium-capable Telethon user session and a private Kaggle
# CPU worker for ffmpeg/chunking. Never reuse E2E/S22/STORY/editor sessions.
PRIVATE_EVENTS_MCP_AUDIO_TRANSCRIPTION_ENABLED=0
# AUDIO_TRANSCRIPTION_ALLOWED_HOSTS=files.oaiusercontent.com,*.blob.core.windows.net
# TELEGRAM_AUTH_BUNDLE_TRANSCRIPTION=
# AUDIO_TRANSCRIPTION_ROOT=/data/audio-transcription
# AUDIO_TRANSCRIPTION_MAX_ASSET_BYTES=536870912
# AUDIO_TRANSCRIPTION_MAX_STORE_BYTES=2147483648
# AUDIO_TRANSCRIPTION_ASSET_TTL_SECONDS=86400
# AUDIO_TRANSCRIPTION_DOWNLOAD_TIMEOUT_SECONDS=120
# AUDIO_TRANSCRIPTION_RESULT_RETENTION_DAYS=7
# AUDIO_TRANSCRIPTION_POLL_INTERVAL_SECONDS=20
# AUDIO_TRANSCRIPTION_MAX_RUN_HOURS=8
# AUDIO_TRANSCRIPTION_KAGGLE_KERNEL_REF=<KAGGLE_USERNAME>/events-bot-audio-transcription
# AUDIO_TRANSCRIPTION_KAGGLE_KERNEL_SOURCE=local:AudioTranscription
# AUDIO_TRANSCRIPTION_AUTH_BUNDLE_ENV=TELEGRAM_AUTH_BUNDLE_TRANSCRIPTION
# AUDIO_TRANSCRIPTION_TELEGRAM_PEER=me
# AUDIO_TRANSCRIPTION_CLEANUP_MESSAGES=1
# AUDIO_TRANSCRIPTION_KEEP_KAGGLE_DATASETS=0
''',
    )

    changelog = REPO / "CHANGELOG.md"
    _replace_once(
        changelog,
        "## [Unreleased]\n",
        '''## [Unreleased]
- **Private Events MCP / Telegram-native audio transcription (default off):**
  added owner-bound audio `fileParams` ingress, durable idempotent jobs, a
  pause-aware CPU Kaggle ffmpeg worker, dedicated guarded Telethon session,
  native Telegram transcription, truthful relative/absolute timestamps and
  digest-verified TXT/timeline/JSON/SRT/VTT exports. Codex remains exact-seven;
  activation still requires a separate Premium-capable session and live canary.
''',
    )

    # Remove the one-shot installer from the resulting branch. The executed
    # workflow may delete itself safely; the final PR then contains only product
    # code, tests and documentation.
    shutil.rmtree(BOOTSTRAP, ignore_errors=True)
    (REPO / "scripts" / "bootstrap_audio_transcription_branch.py").unlink(missing_ok=True)
    (REPO / ".github" / "workflows" / "audio-transcription-bootstrap.yml").unlink(
        missing_ok=True
    )

    print("audio transcription implementation expanded and repository patches applied")


if __name__ == "__main__":
    main()
