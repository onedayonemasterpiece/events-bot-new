from audio_transcription.asset_store import detect_audio_mime


def test_audio_signature_detection():
    assert detect_audio_mime(b"OggS" + b"\0" * 20) == "audio/ogg"
    assert detect_audio_mime(b"fLaC" + b"\0" * 20) == "audio/flac"
    assert detect_audio_mime(b"RIFF\x10\0\0\0WAVEfmt ") == "audio/wav"
    assert detect_audio_mime(b"ID3" + b"\0" * 20) == "audio/mpeg"
    assert detect_audio_mime(b"\0\0\0\x18ftypM4A ") == "audio/mp4"
    assert detect_audio_mime(b"\x1aE\xdf\xa3" + b"\0" * 20) == "audio/webm"
    assert detect_audio_mime(b"not audio") is None
