import pytest

from audio_transcription.asset_store import (
    AudioAssetExpired,
    AudioAssetOwnershipError,
    AudioAssetStore,
    detect_audio_mime,
)


def test_audio_signature_detection():
    assert detect_audio_mime(b"OggS" + b"\0" * 20) == "audio/ogg"
    assert detect_audio_mime(b"fLaC" + b"\0" * 20) == "audio/flac"
    assert detect_audio_mime(b"RIFF\x10\0\0\0WAVEfmt ") == "audio/wav"
    assert detect_audio_mime(b"ID3" + b"\0" * 20) == "audio/mpeg"
    assert detect_audio_mime(b"\0\0\0\x18ftypM4A ") == "audio/mp4"
    assert detect_audio_mime(b"\x1aE\xdf\xa3" + b"\0" * 20) == "audio/webm"
    assert detect_audio_mime(b"not audio") is None


def test_trusted_provider_ingress_is_immutable_owner_bound_deduplicated_and_expires(tmp_path):
    now = [1_900_000_000.0]
    store = AudioAssetStore(
        tmp_path / "audio",
        allowed_hosts=("files.example.test",),
        max_asset_bytes=1024,
        max_store_bytes=4096,
        ttl_seconds=60,
        timeout_seconds=5,
        clock=lambda: now[0],
    )
    values = {
        "owner_binding": "a" * 64,
        "provider_fingerprint": "b" * 64,
        "mime_type": "audio/ogg",
        "display_name": "voice",
    }
    first = store.ingest_provider_media(b"OggS" + b"\0" * 32, **values)
    second = store.ingest_provider_media(b"different bytes are not re-read", **values)
    assert second.storage_ref == first.storage_ref
    assert second.content_digest == first.content_digest
    with pytest.raises(AudioAssetOwnershipError):
        store.reverify(first.storage_ref, owner_binding="c" * 64)
    now[0] += 61
    with pytest.raises(AudioAssetExpired):
        store.reverify(first.storage_ref, owner_binding="a" * 64)
