/** Companion to PCM checkpoints, adapted from WL review's single-recorder,
 * ordered chunk writes and stop-before-seal contract. Chunks are container
 * fragments: concatenate all of them; never decode each fragment separately. */
export type CompressedPart = { index: number; mimeType: string; bytes: Uint8Array };
export type CompressedReceipt = { mimeType: string; partCount: number; bytes: number; complete: boolean; reason?: string };
export const COMPRESSED_MIME_TYPES = ['audio/webm;codecs=opus', 'audio/ogg;codecs=opus', 'audio/mp4;codecs=mp4a.40.2', 'audio/mp4'] as const;

export class CompressedCapture {
  private recorder: MediaRecorder | null = null;
  private tail: Promise<void> = Promise.resolve();
  private stopped: Promise<void>;
  private resolveStopped!: () => void;
  private stopping: Promise<CompressedReceipt> | null = null;
  private requestedStop = false;
  private closed = false;
  private sawStop = false;
  private failure: string | undefined;
  private mimeType = '';
  private partCount = 0;
  private bytes = 0;
  private nextIndex = 0;
  private persist: (part: CompressedPart) => Promise<void>;
  constructor(stream: MediaStream, persist: (part: CompressedPart) => Promise<void>) {
    this.persist = persist;
    this.stopped = new Promise(resolve => { this.resolveStopped = resolve; });
    if (!globalThis.MediaRecorder) { this.failure = 'media_recorder_unavailable'; return; }
    for (const mimeType of COMPRESSED_MIME_TYPES) {
      let recorder: MediaRecorder | null = null;
      try {
        if (!MediaRecorder.isTypeSupported(mimeType)) continue;
        recorder = new MediaRecorder(stream, { mimeType, audioBitsPerSecond: 32000 });
        recorder.ondataavailable = event => {
          if (this.closed || !event.data.size) return;
          const index = this.nextIndex++;
          // Use the produced format, not a codec label inferred from preference.
          const actualMime = event.data.type || recorder!.mimeType;
          this.tail = this.tail.then(async () => {
            try {
              if (!actualMime) throw new Error('compressed_mime_missing');
              if (this.partCount && this.mimeType !== actualMime) throw new Error('compressed_mime_changed');
              this.mimeType = actualMime;
              const bytes = new Uint8Array(await event.data.arrayBuffer());
              await this.persist({ index, mimeType: actualMime, bytes });
              this.partCount++; this.bytes += bytes.byteLength;
            } catch { this.failure = 'compressed_storage_or_data_failed'; }
          });
        };
        recorder.onerror = () => { this.failure = 'compressed_capture_failed'; };
        recorder.onstop = () => {
          if (!this.requestedStop) this.failure = this.failure || 'compressed_interrupted';
          this.sawStop = true; this.resolveStopped();
        };
        recorder.start(1000);
        this.recorder = recorder; this.mimeType = recorder.mimeType;
        return;
      } catch {
        if (recorder) {
          recorder.ondataavailable = recorder.onerror = recorder.onstop = null;
          if (recorder.state !== 'inactive') { try { recorder.stop(); } catch {} }
        }
      }
    }
    this.failure = 'compressed_format_unavailable';
  }
  /** Must be called before stopping the shared stream's tracks. No lifecycle
   * callbacks are shared across recording instances, including timed-out ones. */
  stop(): Promise<CompressedReceipt> {
    if (this.stopping) return this.stopping;
    this.requestedStop = true;
    if (this.recorder && this.recorder.state !== 'inactive') {
      try { this.recorder.stop(); } catch { this.failure = 'compressed_stop_failed'; this.resolveStopped(); }
    } else if (!this.recorder) this.resolveStopped();
    this.stopping = (async () => {
      let timer: ReturnType<typeof setTimeout> | undefined;
      await Promise.race([this.stopped, new Promise<void>(resolve => { timer = setTimeout(() => {
        this.failure = this.failure || 'compressed_stop_timeout'; resolve();
      }, 3000); })]);
      clearTimeout(timer);
      this.closed = true;
      // Native stop dispatches the final dataavailable first. Its durable write
      // is therefore already enqueued, and must settle before exposing a seal.
      await this.tail;
      if (this.recorder) this.recorder.ondataavailable = this.recorder.onerror = this.recorder.onstop = null;
      const complete = this.sawStop && !this.failure && this.partCount > 0 && this.partCount === this.nextIndex;
      return { mimeType: this.mimeType, partCount: this.partCount, bytes: this.bytes, complete,
        ...(!complete ? { reason: this.failure || 'compressed_empty' } : {}) };
    })();
    return this.stopping;
  }
}
