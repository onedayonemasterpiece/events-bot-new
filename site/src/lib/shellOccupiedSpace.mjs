/** Read-only geometry for existing shell consumers. No controller, data order,
 * persistence, telemetry or transport lives here. Disjoint islands remain
 * separate rectangles: their enclosing box would invent occluded content. */
export function shellOccupiedSpace(viewport, entries) {
  const x=Number(viewport?.x ?? 0), y=Number(viewport?.y ?? 0);
  const width=Number(viewport?.width), height=Number(viewport?.height);
  if (![x,y,width,height].every(Number.isFinite) || width<=0 || height<=0)
    return Object.freeze({viewport:null,rects:Object.freeze([])});
  const seen=new Map();
  for (const entry of entries || []) {
    const r=entry?.rect;
    if (!entry?.id || !r || ![r.x,r.y,r.width,r.height].every(Number.isFinite) || r.width<=0 || r.height<=0) continue;
    const left=Math.max(x,r.x),top=Math.max(y,r.y);
    const right=Math.min(x+width,r.x+r.width),bottom=Math.min(y+height,r.y+r.height);
    if(right<=left || bottom<=top)continue;
    seen.set(entry.id,Object.freeze({id:entry.id,role:entry.role || 'chrome',x:left,y:top,width:right-left,height:bottom-top}));
  }
  return Object.freeze({viewport:Object.freeze({x,y,width,height}),rects:Object.freeze([...seen.values()])});
}
