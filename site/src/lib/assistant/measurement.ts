/** Bounded preview/test readout, NOT a new analytics sink or product database.
 * Remote durable action acknowledgements remain owned by savedEventRuntime.
 * No speech, query, profile, token, URL or stable account identifier is collected.
 */
export class AssistantMeasurement {
  private rendered=new Set<string>();private seen=new Set<string>();private actions=new Set<string>();
  private sections=new Set<string>();private firstAt:number|null=null;private firstValueMs:number|null=null;
  private sequence=0;readonly traffic:'preview'|'synthetic';
  constructor(traffic:'preview'|'synthetic'='preview'){this.traffic=traffic;}
  render(section:string,ids:string[],at=performance.now()){
    if(this.firstAt===null)this.firstAt=at;
    if(this.sections.size<256)this.sections.add(section);
    for(const id of ids)if(this.rendered.size<4096)this.rendered.add(id);
  }
  expose(id:string){if(this.rendered.has(id)&&this.seen.size<4096)this.seen.add(id);}
  committed(id:string,kind:'favorite'|'calendar',saved:boolean,at=performance.now()){
    if(!saved||!this.rendered.has(id))return;
    if(this.actions.size<4096)this.actions.add(`${kind}:${id}`);
    if(this.firstValueMs===null&&this.firstAt!==null)this.firstValueMs=Math.max(0,at-this.firstAt);
  }
  readout(){return{contract:'kenigevents.voice-preview-readout.v1',traffic:this.traffic,production_eligible:false,
    sections:this.sections.size,unique_rendered:this.rendered.size,unique_seen:this.seen.size,
    committed_favorites:[...this.actions].filter(x=>x.startsWith('favorite:')).length,
    committed_calendar:[...this.actions].filter(x=>x.startsWith('calendar:')).length,
    first_durable_value_ms:this.firstValueMs,sequence:++this.sequence};}
}
