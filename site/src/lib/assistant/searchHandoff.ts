/** Local transfer metadata. URLs contain only id; payload stays in VoiceStore. */
export const HOME_HANDOFF_VERSION = 'kenigevents.home-search-handoff.v1';
export const HOME_HANDOFF_TTL_MS = 24 * 60 * 60 * 1000;
export type HandoffScope = { origin:string; prefix:string; storageScope:string };
export type HomeHandoff = {
  id:string; kind:'home-handoff-v1'; owner:string; createdAt:string; version:typeof HOME_HANDOFF_VERSION;
  origin:string; prefix:string; submittedAt:string; taskId:string; interpretationId:string; searchId:string; asrId:string;
  payload:{kind:'text';text:string}|{kind:'audio';recordingId:string};
  status:'prepared'|'adopted'|'completed'|'empty'|'cancelled';
};
export const validHandoffId=(id:unknown):id is string=>typeof id==='string'&&/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(id);
export async function handoffScope(origin:string,prefix:string):Promise<HandoffScope>{
  const base=new URL(origin);if(base.origin!==origin||!['http:','https:'].includes(base.protocol))throw Error('handoff_origin_invalid');
  const normalized=prefix.replace(/\/$/,'');
  if(normalized&&(!normalized.startsWith('/')||normalized.startsWith('//')||/[?#\\]|(?:^|\/)\.\.?(?:\/|$)/.test(normalized)))throw Error('handoff_prefix_invalid');
  const bytes=await crypto.subtle.digest('SHA-256',new TextEncoder().encode(`${origin}\n${normalized}`));
  const hash=Array.from(new Uint8Array(bytes),x=>x.toString(16).padStart(2,'0')).join('').slice(0,24);
  return {origin,prefix:normalized,storageScope:`home-v1-${hash}`};
}
export function validateHandoff(row:HomeHandoff|null,owner:string,scope:HandoffScope,now=Date.now()):HomeHandoff{
  if(!row||row.kind!=='home-handoff-v1'||row.version!==HOME_HANDOFF_VERSION||row.owner!==owner||row.origin!==scope.origin||row.prefix!==scope.prefix||![row.id,row.taskId,row.interpretationId,row.searchId,row.asrId].every(validHandoffId))throw Error('handoff_scope_invalid');
  const submitted=Date.parse(row.submittedAt);if(!Number.isFinite(submitted)||submitted>now+60000||now-submitted>HOME_HANDOFF_TTL_MS)throw Error('handoff_expired');
  if(!['prepared','adopted','completed','empty','cancelled'].includes(row.status))throw Error('handoff_invalid');
  if(row.payload?.kind==='text'){if(typeof row.payload.text!=='string'||!row.payload.text.trim()||row.payload.text.length>8192)throw Error('handoff_empty');}
  else if(row.payload?.kind!=='audio'||!validHandoffId(row.payload.recordingId))throw Error('handoff_invalid');
  return row;
}
export function handoffUrl(row:HomeHandoff,scope:HandoffScope):string{
  return `${scope.prefix}/poisk/?voice_handoff=${encodeURIComponent(row.id)}`;
}
