/** Presentation only: never accepts input or dispatches provider requests. */
const element=<K extends keyof HTMLElementTagNameMap>(tag:K,cls:string,text?:string)=>{
  const el=document.createElement(tag);el.className=cls;if(text!==undefined)el.textContent=text;return el;
};
export function createConversationTurn(question:string){
  const section=element('section','assistant__turn');section.dataset.assistantTurn='pending';
  const message=element('div','assistant__user-message');
  message.append(element('span','assistant__speaker','Вы'));
  const text=element('p','',question);text.dataset.assistantQuestion='';message.append(text);
  const body=element('div','assistant__response');body.dataset.assistantResponse='';
  const status=element('p','assistant__turn-status','Подбираю события…');status.setAttribute('role','status');
  const skeleton=element('div','assistant__skeleton');skeleton.dataset.assistantSkeleton='';skeleton.setAttribute('aria-hidden','true');
  const lines=element('div','assistant__skeleton-lines');
  for(let i=0;i<3;i++)lines.append(element('span','ke-skeleton'));
  const cards=element('div','assistant__skeleton-cards');
  for(let i=0;i<3;i++){
    const card=element('div','assistant__skeleton-card');card.append(element('div','ke-skeleton assistant__skeleton-poster'),element('span','ke-skeleton'),element('span','ke-skeleton'));cards.append(card);
  }
  skeleton.append(lines,cards);body.append(status,skeleton);body.setAttribute('aria-busy','true');section.append(message,body);
  return {section,body,status};
}
export type ConversationTurn=ReturnType<typeof createConversationTurn>;
export function settleConversationTurn(turn:ConversationTurn,state:'ready'|'error'|'superseded',message=''){
  turn.section.dataset.assistantTurn=state;turn.body.setAttribute('aria-busy','false');turn.body.replaceChildren();
  if(message){turn.status.textContent=message;turn.body.append(turn.status);}
}
