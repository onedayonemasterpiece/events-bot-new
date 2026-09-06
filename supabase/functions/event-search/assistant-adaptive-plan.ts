import { object, reject, text } from './assistant-intent.ts';

/** Routing is model-led; this module enforces shape/capabilities, never keywords. */
export type AdaptivePlan = {
  knowledgeAction:'internal'|'web_lookup';
  externalNeed:string|null;
  externalQuery:string|null;
  clarification:'none'|'blocking'|'optional';
  question:string|null;
  assumptions:string[];
  refinementOpportunity:string|null;
};
const nullableText={type:['string','null'],maxLength:320};
export const ADAPTIVE_PLAN_SCHEMA={type:'object',additionalProperties:false,
  required:['knowledgeAction','externalNeed','externalQuery','clarification','question','assumptions','refinementOpportunity'],properties:{
    knowledgeAction:{type:'string',enum:['internal','web_lookup']},externalNeed:nullableText,externalQuery:nullableText,
    clarification:{type:'string',enum:['none','blocking','optional']},question:nullableText,
    assumptions:{type:'array',maxItems:2,items:{type:'string',maxLength:240}},refinementOpportunity:nullableText,
  }};
export function validateAdaptivePlan(value:unknown):AdaptivePlan {
  const p=object(value,ADAPTIVE_PLAN_SCHEMA.required);
  if(!['internal','web_lookup'].includes(p.knowledgeAction)||!['none','blocking','optional'].includes(p.clarification))reject('invalid_adaptive_plan');
  for(const key of ['externalNeed','externalQuery','question','refinementOpportunity'])if(p[key]!==null)text(p[key],320);
  if(!Array.isArray(p.assumptions)||p.assumptions.length>2)reject('invalid_adaptive_plan');
  for(const assumption of p.assumptions)text(assumption,240);
  if((p.knowledgeAction==='internal')!==(p.externalNeed===null&&p.externalQuery===null))reject('invalid_adaptive_plan');
  if(p.knowledgeAction==='web_lookup'&&(!p.externalNeed||!p.externalQuery))reject('invalid_adaptive_plan');
  if((p.clarification==='none')!==(p.question===null))reject('invalid_adaptive_plan');
  // Grounding execution is not enabled. Never pretend an unresolved external
  // reference was looked up, nor pay for retrieval under invented constraints.
  if(p.knowledgeAction==='web_lookup'&&p.clarification!=='blocking')reject('web_grounding_unavailable');
  return p as AdaptivePlan;
}
export const adaptivePlannerPrompt=`В ЭТОМ ЖЕ ответе заполни adaptivePlan; отдельного вызова маршрутизатора нет.
Различай неопределённость желания пользователя, пробел внешнего знания и просто сложную формулировку. Длина вопроса, нулевая предыдущая выдача и сложность сами по себе НЕ требуют интернета или уточнения.
knowledgeAction=internal по умолчанию: ищем только во внутреннем каталоге. Если конкретный неизвестный внешний факт действительно необходим, выбери web_lookup, externalNeed=какой факт неизвестен, externalQuery=минимальный обезличенный запрос без истории/личных данных. Иначе оба null. Уже известный тебе смысл названия/жанра не требует web_lookup.
CAPABILITIES: web execution unavailable. Для web_lookup НЕ выдумывай найденные факты и условия: clarification=blocking, question=один естественный вопрос, позволяющий пользователю пояснить неизвестную отсылку. Это запланированный, но НЕ выполненный веб-поиск. События/ID/даты/наличие берём только из внутренней базы.
clarification=blocking ТОЛЬКО если без ответа вероятен принципиально неверный поиск; question — один короткий конкретный вопрос. Не требуй жанр/город/бюджет просто потому что они не названы. Тогда верхнее поле clarification равно question, queryPlan=null: исполнимых условий ещё нет; поиск не выполняется. Не заполняй выдуманный поисковый план ради вопроса. После ответа на уточнение, если BASE_QUERY_PLAN=null, собери replace из полного QUERY_INPUT с исходным вопросом и новым ответом. Для optional/none queryPlan обязателен и НЕ null.
clarification=optional если уже можно дать полезные события, но ответ пользователя существенно улучшит следующий поиск. question не должен заявлять наличие/отсутствие результатов ДО поиска. Верхнее поле clarification=null; поиск НЕ блокируется. Например запрос с детьми без возраста обычно допускает семейную подборку сейчас и вопрос о возрасте, но не все семейные запросы требуют его повторения.
clarification=none и question=null если уточнять незачем. Никакого обязательного вопроса в конце каждого ответа. Уже отвеченное уточнение не повторяй: CONVERSATION_CONTEXT содержит прежний вопрос, INPUT — текущий ответ.
assumptions — до двух важных предположений, которые действительно нужно сообщить, иначе []; не перечисляй механику и очевидные умолчания. refinementOpportunity — одна возможная идея улучшить выборку для редактора ПОСЛЕ поиска, иначе null. Это гипотеза, не обещание: редактор должен проверить её пользу по реальным результатам. Не предсказывай их количество, не расширяй/сужай условия молча.`;

/** Optional clarifications/assumptions remain part of the existing answer before
 * cards, not another modal/footer. Persist once; status doesn't append again. */
export function adaptiveAnswer(answer:string, plan?:AdaptivePlan|null):string {
  if(!plan||plan.clarification==='blocking')return answer;
  return [...plan.assumptions,answer,plan.clarification==='optional'?plan.question:null].filter(Boolean).join('\n');
}
