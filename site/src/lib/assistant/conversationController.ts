import { initialState, acceptInput, interpretInput, retrievalTicket, ticketIsCurrent, commitAnswer, resetTask,
  type State, type Section, type Mode } from './conversationState.ts';
import type { AssistantClient, Command, RemoteReceipt } from './assistantClient.ts';
export interface ConversationStorage {
  conversation(owner:string):Promise<State|null>;
  checkpoint(owner:string,state:State,expected:number,command?:Command):Promise<void>;
  saveAnswer(owner:string,id:string,payload:Record<string,unknown>):Promise<void>;
  command(owner:string,id:string):Promise<Command|null>;
}
export type ConversationEvents = {change:(state:State)=>void;answer:(result:any,visible:boolean)=>void;status:(message:string)=>void;error:(error:unknown)=>void};
/** Durable ordered host for the portable kernel. Capture never waits on this
 * queue. State transitions and network processing have independent serial lanes.
 */
export class ConversationController {
  state:State=initialState();readonly owner:string;
  private store:ConversationStorage;private api:Pick<AssistantClient,'control'|'execute'|'status'>;private events:ConversationEvents;
  private transitions:Promise<unknown>=Promise.resolve();private pipeline:Promise<unknown>=Promise.resolve();
  constructor(owner:string,store:ConversationStorage,api:Pick<AssistantClient,'control'|'execute'|'status'>,events:ConversationEvents){this.owner=owner;this.store=store;this.api=api;this.events=events;}
  async initialize(){this.state=(await this.store.conversation(this.owner))||initialState();this.events.change(this.state);}
  private update<T>(change:(state:State)=>{state:State;value:T;command?:Command}):Promise<T>{
    const task=this.transitions.then(async()=>{
      const current=this.state;const next=change(current);
      if(next.state!==current){await this.store.checkpoint(this.owner,next.state,current.revision,next.command);this.state=next.state;this.events.change(this.state);}
      return next.value;
    });
    this.transitions=task.catch(()=>{});return task;
  }
  async submit(text:string,mode:Mode,parentId:string|null,visibleIds:string[],anchor=new Date().toISOString()):Promise<Command>{
    const command=await this.update(state=>{
      // The bounded kernel is a working set, not the archive. Inputs/answers
      // already live in separate owner-scoped durable records.
      let working=state;
      if(!state.draft&&(state.sections.length>=64||state.receipts.length>=250)){
        const kept=state.sections.filter(section=>section.id===parentId||state.sections.indexOf(section)>=state.sections.length-40);
        working=resetTask({...state,sections:kept},false);
      }
      const actualMode=working.draft?'continue_draft':mode;
      const actualParent=working.draft?working.draft.parentId:parentId;
      const input={id:crypto.randomUUID(),sequence:working.acceptedThrough+1,epoch:working.epoch,
        previousId:working.receipts.at(-1)?.id||null,mode:actualMode,parentId:actualParent,text};
      const cmd:Command={id:input.id,searchId:crypto.randomUUID(),input,createdAt:new Date().toISOString(),
        payload:{text,mode:actualMode,parentId:actualParent,previousId:input.previousId,anchor,visibleIds}};
      return{state:acceptInput(working,input),value:cmd,command:cmd};
    });
    this.events.status('Текст сохранён на устройстве. Передаю подтверждённый ввод.');
    // Intake can run while the previous provider stage is pending. Attach the
    // rejection handler immediately; processing still follows acceptance order.
    const intake=this.api.control(this.owner,command.id,'interpret',command.payload,false).then(()=>null,error=>error);
    this.pipeline=this.pipeline.then(async()=>{
      const intakeError=await intake;
      if(intakeError){this.events.status('Ввод сохранён; проверяю подтверждение приёма.');}
      await this.process(command);
    }).catch(error=>this.events.error(error));
    return command;
  }
  private terminal(receipt:RemoteReceipt):any {
    if(receipt.state!=='completed')throw new Error(receipt.state==='outcome_unknown'?'voice_outcome_unknown':receipt.error||`voice_${receipt.waiting||receipt.state}`);
    if(!receipt.result||typeof receipt.result!=='object')throw new Error('voice_result_invalid');
    return receipt.result;
  }
  private async process(command:Command):Promise<void>{
    const row=this.state.receipts.find(r=>r.id===command.id);
    if(!row||row.epoch!==this.state.epoch)return; // Explicit new task, not deletion.
    if(row.status==='accepted'){
      if(row.sequence!==this.state.processedThrough+1)throw new Error('voice_predecessor_pending');
      this.events.status('Понимаю подтверждённый запрос.');
      const interpreted=this.terminal(await this.api.execute(this.owner,command.id,'interpret',command.payload));
      await this.update(state=>{
        if(state.epoch!==command.input.epoch)return{state,value:null};
        const receipt=state.receipts.find(r=>r.id===command.id);
        if(receipt?.status==='interpreted')return{state,value:null};
        return{state:interpretInput(state,command.id,interpreted.intent,state.revision),value:null};
      });
    }
    if(this.state.epoch!==command.input.epoch||this.state.draft?.status!=='ready'||this.state.processedThrough!==command.input.sequence)return;
    const ticket=retrievalTicket(this.state);
    this.events.status('Подбираю события в текущем каталоге.');
    const result=this.terminal(await this.api.execute(this.owner,command.searchId,'search',{interpretationId:command.id}));
    if(!Array.isArray(result.items)||result.items.length>60||result.id!==command.searchId||typeof result.title!=='string'||typeof result.answer!=='string'||typeof result.catalog_revision!=='string')throw new Error('voice_result_invalid');
    await this.store.saveAnswer(this.owner,result.id,result);
    const visible=await this.update(state=>{
      if(!ticketIsCurrent(state,ticket))return{state,value:false};
      const ids=result.items.map((item:any)=>String(item.event_id??item.id));
      const next=commitAnswer(state,ticket,{id:result.id,title:result.title,answer:result.answer,eventIds:ids,catalogRevision:result.catalog_revision},ids);
      return{state:next,value:true};
    });
    this.events.answer(result,visible);
    this.events.status(visible?'Ответ сохранён. Можно уточнить условия или начать новый поиск.':'Сохранён предыдущий ответ; обрабатываю уточнение.');
  }
  /** User-initiated recovery, not auto-submit on login/reload. A completed
   * receipt is reused; the server refuses re-dispatch of sent/unknown stages. */
  resume():Promise<void>{
    const queued=this.pipeline.then(async()=>{
      for(const receipt of [...this.state.receipts]){
        if(!this.state.draft)break;
        if(receipt.epoch!==this.state.epoch)continue;
        const command=await this.store.command(this.owner,receipt.id);
        if(!command)throw new Error('voice_local_receipt_missing');
        if(receipt.status==='accepted'||receipt.sequence===this.state.processedThrough)await this.process(command);
      }
    });
    this.pipeline=queued.catch(error=>this.events.error(error));return this.pipeline as Promise<void>;
  }
  async newTask(){await this.update(state=>({state:resetTask(state,false),value:null}));}
  /** Loading archived history must not replace the viewed/base/pending target. */
  async rememberSection(section:Section){await this.update(state=>{
    if(state.sections.some(item=>item.id===section.id))return{state,value:null};
    const sections=state.sections.length>=63?state.sections.slice(-48):state.sections;
    return{state:{...state,sections:[...sections,structuredClone(section)],revision:state.revision+1},value:null};
  });}
  settled():Promise<unknown>{return this.pipeline;}
}
