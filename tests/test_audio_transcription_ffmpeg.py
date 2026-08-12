import math
import struct
import wave

from audio_transcription.contracts import AudioChunk
from audio_transcription.ffmpeg import probe_audio, transcode_chunk_to_voice


def test_ffmpeg_probe_and_voice_transcode(tmp_path):
    source = tmp_path / "source.wav"
    sample_rate = 16_000
    with wave.open(str(source), "wb") as output:
        output.setnchannels(1)
        output.setsampwidth(2)
        output.setframerate(sample_rate)
        frames = bytearray()
        for index in range(sample_rate * 2):
            value = int(4000 * math.sin(2 * math.pi * 440 * index / sample_rate))
            frames.extend(struct.pack("<h", value))
        output.writeframes(bytes(frames))

    probe = probe_audio(source)
    assert 1_900 <= probe.duration_ms <= 2_100
    assert probe.channels == 1

    voice = transcode_chunk_to_voice(
        source,
        AudioChunk(index=0, start_ms=0, end_ms=probe.duration_ms),
        tmp_path / "voice.ogg",
    )
    assert voice.read_bytes().startswith(b"OggS")
