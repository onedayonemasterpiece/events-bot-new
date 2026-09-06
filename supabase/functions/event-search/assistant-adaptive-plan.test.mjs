import test from 'node:test';import assert from 'node:assert/strict';
import {validateAdaptivePlan,adaptiveAnswer,adaptivePlannerPrompt} from './assistant-adaptive-plan.ts';
import {editorialSchema,editorialPrompt,validateEditorial,editorialText} from './assistant-editorial.ts';
const plan=(extra={})=>({knowledgeAction:'internal',externalNeed:null,externalQuery:null,clarification:'none',question:null,assumptions:[],refinementOpportunity:null,...extra});
const cards=[{event_id:1,title:'Лекция',search_digest:'Рассказ об истории края'}];
const editorial=(extra={})=>({intro:'Обратите внимание на лекцию.',recommendations:[{event_id:1,comment:'Об истории края.',evidence_index:0}],...extra});
test('strict adaptive shape rejects missing fields, contradictions and oversized content',()=>{
 for(const bad of [null,{},plan({extra:1}),plan({question:'Что?'}),plan({clarification:'optional'}),plan({knowledgeAction:'guess'}),plan({externalNeed:'fact'}),plan({assumptions:['a','b','c']}),plan({question:undefined}),plan({refinementOpportunity:'a'.repeat(321)})])assert.throws(()=>validateAdaptivePlan(bad));
 assert.deepEqual(validateAdaptivePlan(plan()),plan());
});
test('web capability gate cannot produce an ungrounded successful route',()=>{
 assert.throws(()=>validateAdaptivePlan(plan({knowledgeAction:'web_lookup',externalNeed:'факт',externalQuery:'запрос'})),/web_grounding_unavailable/);
});
test('optional answer and important assumptions are inline, no footer or repeated CTA',()=>{
 assert.equal(adaptiveAnswer('Ответ',plan({assumptions:['Предположу, что подойдут семейные события.'],clarification:'optional',question:'Сколько лет ребёнку?'})),'Предположу, что подойдут семейные события.\nОтвет\nСколько лет ребёнку?');
 assert.equal(adaptiveAnswer('Ответ',plan()),'Ответ');assert.equal(adaptiveAnswer('Вопрос',plan({clarification:'blocking',question:'Вопрос'})),'Вопрос');
 assert.match(adaptivePlannerPrompt,/Длина вопроса/);
});
test('editorial decides refinement after actual candidates; null is a complete answer',()=>{
 assert.ok(editorialSchema(cards,true).required.includes('refinement'));
 const value=validateEditorial(editorial({refinement:null}),cards,true);assert.ok(!editorialText(value,cards).includes('null'));
 assert.match(editorialPrompt('Лекции',{},cards,plan({refinementOpportunity:'Можно ли уточнить тему?'}),8),/"resultCount":8/);
 assert.equal(editorialText(validateEditorial(editorial({refinement:'Можно сузить поиск до истории региона.'}),cards,true),cards).split('\n').at(-1),'Можно сузить поиск до истории региона.');
 assert.throws(()=>validateEditorial(editorial(),cards,true));assert.throws(()=>validateEditorial(editorial({refinement:'a'.repeat(241)}),cards,true));
 // Historical editorial receipts do not require the new optional field.
 assert.equal(validateEditorial(editorial(),cards).intro,'Обратите внимание на лекцию.');
});
test('optional planner question prevents a second editorial refinement in schema and validation',()=>{
 assert.deepEqual(editorialSchema(cards,true,false).properties.refinement.enum,[null]);
 assert.throws(()=>validateEditorial(editorial({refinement:'Ещё вопрос?'}),cards,true,false),/duplicate_refinement/);
});
