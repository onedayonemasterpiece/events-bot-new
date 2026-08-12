from __future__ import annotations

import json
import math
import struct
import tempfile
import wave
from pathlib import Path

from audio_transcription.chunking import plan_chunks
from audio_transcription.contracts import AudioChunk, Precision
from audio_transcription.ffmpeg import probe_audio, transcode_chunk_to_voice


def main() -> None:
    repo = Path(__file__).resolve().parents[1]
    metadata = json.loads(
        (repo / "kaggle" / "AudioTranscription" / "kernel-metadata.json").read_text(
            encoding="utf-8"
        )
    )
    assert metadata["code_file"] == "audio_transcription.py"
    assert metadata["enable_gpu"] is False
    assert metadata["enable_internet"] is True

    with tempfile.TemporaryDirectory(prefix="audio-transcription-smoke-") as temporary:
        root = Path(temporary)
        source = root / "fixture.wav"
        sample_rate = 16_000
        with wave.open(str(source), "wb") as output:
            output.setnchannels(1)
            output.setsampwidth(2)
            output.setframerate(sample_rate)
            frames = bytearray()
            for index in range(sample_rate * 3):
                value = int(3500 * math.sin(2 * math.pi * 330 * index / sample_rate))
                frames.extend(struct.pack("<h", value))
            output.writeframes(bytes(frames))
        probe = probe_audio(source)
        chunks = plan_chunks(probe.duration_ms, [], precision=Precision.PHRASE)
        voice = transcode_chunk_to_voice(
            source,
            AudioChunk(index=0, start_ms=0, end_ms=probe.duration_ms),
            root / "fixture.ogg",
        )
        assert voice.read_bytes().startswith(b"OggS")
    print(
        json.dumps(
            {
                "ok": True,
                "duration_ms": probe.duration_ms,
                "chunks": len(chunks),
                "kernel": metadata["id"],
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
