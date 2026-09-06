import type { AudioPart, WireBudget } from './audioSegments.ts';
import { StreamingPcm16 } from './streamingAudio.ts';
import { CompressedCapture, type CompressedPart, type CompressedReceipt } from './compressedCapture.ts';
import { SilenceEndpointDetector } from './silenceEndpoint.ts';

export type CaptureReason = 'user' | 'silence' | 'background' | 'device_lost' | 'interrupted' |
  'storage_backpressure' | 'storage_failed' | 'capture_failed' | 'cancelled' | 'flush_timeout';
export type CaptureStatus = 'idle' | 'requesting' | 'recording' | 'stopping' | 'saved' | 'partial' | 'error';
export type CaptureReceipt = { reason: CaptureReason; sampleRate: number; frames: number;
  savedFrames: number; partCount: number; complete: boolean; captureComplete?: boolean; compressed?: CompressedReceipt };
export type CaptureOptions = {
  workletUrl: string;
  budget: WireBudget;
  /** Must resolve only after durable commit (e.g. an IndexedDB transaction). */
  onPart: (part: AudioPart) => Promise<void>;
  /** Optional durable companion; failures never discard or stop PCM capture. */
  onCompressedPart?: (part: CompressedPart) => Promise<void>;
  onStatus: (state: CaptureStatus, reason?: string) => void;
  onStopped?: (receipt: CaptureReceipt) => void;
  maxPendingBytes?: number;
  /** Default on. Observation only: never trims PCM or compressed bytes. */
  silenceDetection?: boolean;
};

/** Actual browser capture. No provider, network, auth client or speech recognition
 * hidden here. The caller explicitly requests transcription after local save.
 */
export class MicrophoneCapture {
  status: CaptureStatus = 'idle';
  private generation = 0;
  private context: AudioContext | null = null;
  private stream: MediaStream | null = null;
  private node: AudioWorkletNode | null = null;
  private segmenter: StreamingPcm16 | null = null;
  private compressedCapture: CompressedCapture | null = null;
  private tail: Promise<void> = Promise.resolve();
  private pendingBytes = 0;
  private savedFrames = 0;
  private parts = 0;
  private failedParts: AudioPart[] = [];
  private storageFailed = false;
  private captureFailed = false;
  private acknowledge: (() => void) | null = null;
  private stopping: Promise<CaptureReceipt> | null = null;
  private lastReceipt: CaptureReceipt | null = null;
  private retrying: Promise<number> | null = null;
  private silenceDetector = new SilenceEndpointDetector();
  private requestedStopReason: CaptureReason | null = null;
  private visibility = () => { if (document.hidden) void this.stop('background'); };
  private pagehide = () => { void this.stop('background'); };
  private options: CaptureOptions;
  constructor(options: CaptureOptions) { this.options = options; }
  private setStatus(status: CaptureStatus, reason?: string): void {
    this.status = status; this.options.onStatus(status, reason);
  }
  async start(): Promise<void> {
    if (['requesting', 'recording', 'stopping'].includes(this.status)) throw new Error('capture_busy');
    if (this.retrying) throw new Error('capture_busy');
    if (this.failedParts.length) throw new Error('unsaved_audio');
    if (!globalThis.isSecureContext || !navigator.mediaDevices?.getUserMedia || !globalThis.AudioWorkletNode) {
      this.setStatus('error', 'microphone_unavailable'); throw new Error('microphone_unavailable');
    }
    const generation = ++this.generation;
    this.setStatus('requesting');
    this.lastReceipt = null; this.stopping = null;
    this.requestedStopReason = null; this.silenceDetector.reset();
    this.savedFrames = 0; this.parts = 0; this.pendingBytes = 0; this.storageFailed = false; this.captureFailed = false;
    this.tail = Promise.resolve();
    this.node = null; this.segmenter = null; this.stream = null; this.compressedCapture = null;
    let context: AudioContext | null = null;
    let stream: MediaStream | null = null;
    try {
      context = new AudioContext();
      this.context = context;
      // Resume in the user gesture, before waiting for the permission dialogue.
      const resumed = context.resume().catch(() => undefined);
      const activeContext = context;
      stream = await navigator.mediaDevices.getUserMedia({ audio: { channelCount: 1 }, video: false });
      if (generation !== this.generation) { stream.getTracks().forEach(t => t.stop()); await context.close(); return; }
      this.stream = stream;
      await context.audioWorklet.addModule(this.options.workletUrl);
      if (generation !== this.generation) { stream.getTracks().forEach(t => t.stop()); if (context.state !== 'closed') await context.close(); return; }
      this.segmenter = new StreamingPcm16(context.sampleRate, this.options.budget, context.sampleRate);
      const node = new AudioWorkletNode(context, 'kenig-voice-pcm');
      this.node = node;
      node.port.onmessage = ({ data }) => {
        if (generation !== this.generation) return;
        if (data?.type === 'stopped') {
          if (data.frames === this.segmenter!.frames && data.sampleRate === activeContext.sampleRate) this.acknowledge?.();
          return;
        }
        if (data?.type !== 'pcm') return;
        try {
          if (data.sampleRate !== activeContext.sampleRate) throw new Error('mixed_sample_rates');
          for (const part of this.segmenter!.push(data.pcm, data.firstFrame)) this.persist(part);
          // Analyze only after every received byte entered the original PCM
          // path. Endpointing cannot drop the current block or either tail.
          if (this.status === 'recording' && this.options.silenceDetection !== false) {
            if (document.hidden) void this.stop('background');
            else if (activeContext.state === 'running' && this.silenceDetector.push(data.pcm, data.sampleRate)) void this.stop('silence');
            else if (activeContext.state !== 'running') this.silenceDetector.reset();
          }
        } catch { this.captureFailed = true; void this.stop('capture_failed'); }
      };
      node.onprocessorerror = () => { if (generation !== this.generation) return; this.captureFailed = true; void this.stop('capture_failed'); };
      if (this.options.onCompressedPart) this.compressedCapture = new CompressedCapture(stream, this.options.onCompressedPart);
      const muted = context.createGain(); muted.gain.value = 0;
      context.createMediaStreamSource(stream).connect(node); node.connect(muted); muted.connect(context.destination);
      await resumed; await context.resume();
      if (generation !== this.generation || this.status !== 'requesting') return;
      stream.getAudioTracks().forEach(track => { track.onended = () => { if (generation === this.generation) void this.stop('device_lost'); }; });
      context.onstatechange = () => {
        if (generation === this.generation && activeContext.state !== 'running' && this.status === 'recording') void this.stop('interrupted');
      };
      document.addEventListener('visibilitychange', this.visibility);
      window.addEventListener('pagehide', this.pagehide);
      this.setStatus('recording');
      if (document.hidden) void this.stop('background');
    } catch (error) {
      if (generation === this.generation) await this.compressedCapture?.stop();
      stream?.getTracks().forEach(t => t.stop());
      if (context && context.state !== 'closed') await context.close().catch(() => undefined);
      if (generation === this.generation && this.status === 'requesting') {
        this.setStatus('error', error instanceof DOMException && error.name === 'NotAllowedError' ? 'microphone_denied' : error instanceof DOMException && error.name === 'NotFoundError' ? 'microphone_not_found' : 'capture_failed');
      }
      if (generation !== this.generation || this.status !== 'error') return;
      throw new Error('capture_start_failed');
    }
  }
  private persist(part: AudioPart): void {
    this.parts++; this.pendingBytes += part.bytes.byteLength;
    this.tail = this.tail.then(async () => {
      try { await this.options.onPart(part); this.savedFrames += part.frameCount; }
      catch { this.failedParts.push(part); this.storageFailed = true; void this.stop('storage_failed'); }
      finally { this.pendingBytes -= part.bytes.byteLength; }
    });
    if (this.pendingBytes > (this.options.maxPendingBytes ?? 2 * this.options.budget.maxWireBytes)) void this.stop('storage_backpressure');
  }
  /** Unsaved bytes remain in memory on local storage failure; never clear audio
   * or a database to recover. This retries local storage only, not the provider.
   */
  retryUnsaved(): Promise<number> {
    if (this.retrying) return this.retrying;
    if (['recording', 'requesting', 'stopping'].includes(this.status)) return Promise.reject(new Error('capture_busy'));
    // One retry owns the failed list: concurrent clicks must not double-count
    // the same durable frames or lose a still-failed part.
    this.retrying = (async () => {
      const remaining: AudioPart[] = [];
      for (const part of this.failedParts) {
        try { await this.options.onPart(part); this.savedFrames += part.frameCount; }
        catch { remaining.push(part); }
      }
      this.failedParts = remaining;
      if (this.lastReceipt) {
        this.lastReceipt = { ...this.lastReceipt, savedFrames: this.savedFrames,
          complete: this.lastReceipt.captureComplete === true && !remaining.length && this.savedFrames === this.lastReceipt.frames };
        this.setStatus(this.lastReceipt.complete ? 'saved' : 'partial', remaining.length ? 'storage_failed' : this.lastReceipt.reason);
      }
      return remaining.length;
    })().finally(() => { this.retrying = null; });
    return this.retrying;
  }
  receipt(): CaptureReceipt | null { return this.lastReceipt ? { ...this.lastReceipt } : null; }
  unsavedParts(): readonly AudioPart[] { return this.failedParts; }
  stop(reason: CaptureReason = 'user'): Promise<CaptureReceipt> {
    if (this.lastReceipt) return Promise.resolve(this.lastReceipt);
    if (reason === 'silence' && document.hidden) reason = 'background';
    if (this.stopping) {
      // A lifecycle/device interruption racing the automatic flush is not a
      // completed utterance. Manual user stop retains its existing semantics.
      if (this.requestedStopReason === 'silence' && ['background', 'device_lost', 'interrupted', 'cancelled'].includes(reason)) this.requestedStopReason = reason;
      return this.stopping;
    }
    if (!this.node || !this.segmenter) {
      ++this.generation;
      const compressed = this.compressedCapture; this.compressedCapture = null;
      void compressed?.stop();
      this.stream?.getTracks().forEach(t => t.stop());
      if (this.context && this.context.state !== 'closed') void this.context.close().catch(() => undefined);
      const receipt: CaptureReceipt = { reason: 'cancelled', sampleRate: 0, frames: 0, savedFrames: 0, partCount: 0, complete: false };
      this.lastReceipt = receipt; this.setStatus('idle'); this.options.onStopped?.(receipt); return Promise.resolve(receipt);
    }
    if (this.status === 'requesting') reason = 'cancelled';
    this.requestedStopReason = reason;
    this.setStatus('stopping', reason);
    // Set the promise before its body so persist() cannot recursively start stop.
    this.stopping = Promise.resolve().then(async () => {
      let acknowledged = false;
      const compressedStop = this.compressedCapture?.stop();
      await new Promise<void>(resolve => {
        const timer = setTimeout(() => { this.acknowledge = null; resolve(); }, 1500);
        this.acknowledge = () => { acknowledged = true; clearTimeout(timer); this.acknowledge = null; resolve(); };
        this.node!.port.postMessage({ type: 'stop' });
        this.stream?.getTracks().forEach(t => { t.onended = null; });
      });
      ++this.generation;
      for (const part of this.segmenter!.finish()) this.persist(part);
      await this.tail;
      const compressed = await compressedStop;
      // WL contract: let MediaRecorder emit its final container bytes before
      // releasing the shared input. The worklet has already acknowledged stop.
      this.stream?.getTracks().forEach(t => t.stop());
      document.removeEventListener('visibilitychange', this.visibility);
      window.removeEventListener('pagehide', this.pagehide);
      this.node!.disconnect(); this.node!.port.close(); this.node = null;
      if (this.context && this.context.state !== 'closed') await this.context.close().catch(() => undefined);
      const finalReason = this.requestedStopReason === 'silence' && document.hidden
        ? 'background' : this.requestedStopReason ?? reason;
      const acceptedEnd = finalReason === 'user' || finalReason === 'silence';
      const receipt: CaptureReceipt = {
        ...(compressed ? { compressed } : {}),
        reason: this.storageFailed ? 'storage_failed' : this.captureFailed ? 'capture_failed' : !acknowledged ? 'flush_timeout' : finalReason,
        sampleRate: this.segmenter!.sampleRate, frames: this.segmenter!.frames,
        savedFrames: this.savedFrames, partCount: this.parts,
        captureComplete: acknowledged && !this.captureFailed && acceptedEnd && this.segmenter!.frames > 0,
        complete: acknowledged && !this.captureFailed && !this.storageFailed && acceptedEnd && this.segmenter!.frames > 0,
      };
      this.lastReceipt = receipt;
      this.setStatus(receipt.complete ? 'saved' : 'partial', receipt.reason);
      this.options.onStopped?.(receipt);
      return receipt;
    });
    return this.stopping;
  }
}
