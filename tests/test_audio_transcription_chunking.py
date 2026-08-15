from audio_transcription.chunking import plan_chunks, split_chunk_near_middle
from audio_transcription.contracts import AudioChunk, Precision


def test_phrase_chunks_choose_nearby_pauses_and_cover_source_exactly():
    chunks = plan_chunks(
        205_000,
        [43_000, 88_000, 132_000, 177_000],
        precision=Precision.PHRASE,
    )
    assert [(item.start_ms, item.end_ms) for item in chunks] == [
        (0, 43_000),
        (43_000, 88_000),
        (88_000, 132_000),
        (132_000, 177_000),
        (177_000, 205_000),
    ]
    assert chunks[0].start_ms == 0
    assert chunks[-1].end_ms == 205_000
    assert all(left.end_ms == right.start_ms for left, right in zip(chunks, chunks[1:]))


def test_segment_chunks_never_exceed_hard_max_without_pauses():
    chunks = plan_chunks(610_000, [], precision=Precision.SEGMENT)
    assert max(item.duration_ms for item in chunks) <= 240_000
    assert chunks[-1].end_ms == 610_000


def test_recursive_split_prefers_pause_near_middle():
    chunk = AudioChunk(index=0, start_ms=100_000, end_ms=200_000)
    left, right = split_chunk_near_middle(chunk, [147_000, 180_000])
    assert left.end_ms == 147_000
    assert right.start_ms == 147_000
    assert left.start_ms == chunk.start_ms
    assert right.end_ms == chunk.end_ms
