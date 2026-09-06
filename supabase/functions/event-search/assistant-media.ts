import { validateAudioParts } from './assistant-audio.ts';
import { AUDIO_BUDGET, reject } from './assistant-intent.ts';
export const sha256=async(bytes:Uint8Array)=>Array.from(new Uint8Array(await crypto.subtle.digest('SHA-256',bytes as BufferSource)),x=>x.toString(16).padStart(2,'0')).join('');
export async function assemble(parts:any[],expected:any):Promise<Uint8Array> {
  if(parts.length!==expected.partCount)reject('audio_incomplete',409);
  let frames=0;const ordered=[...parts].sort((a,b)=>a.index-b.index);
  for(const [index,part]of ordered.entries()) {
    if(part.index!==index||part.firstFrame!==frames||part.sampleRate!==expected.sampleRate)reject('audio_gap_or_overlap',409);
    validateAudioParts([{...part,index:0,firstFrame:0}],part.frameCount,AUDIO_BUDGET);
    if(await sha256(part.bytes)!==part.digest)reject('audio_digest_mismatch',409);
    frames+=part.frameCount;
  }
  if(frames!==expected.frames)reject('audio_frame_mismatch',409);
  // All parts are uncompressed PCM16 at one real rate; join payloads, not WAV
  // containers, before the single ASR request. No word-boundary transcript merge.
  const wav=new Uint8Array(44+frames*2);wav.set(ordered[0].bytes.subarray(0,44));
  const view=new DataView(wav.buffer);view.setUint32(4,wav.length-8,true);view.setUint32(40,wav.length-44,true);
  let offset=44;for(const part of ordered){wav.set(part.bytes.subarray(44),offset);offset+=part.bytes.length-44;}
  return wav;
}
