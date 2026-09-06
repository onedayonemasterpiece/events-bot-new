import test from 'node:test';import assert from 'node:assert/strict';
import {editorialSchema,editorialPrompt,validateEditorial,editorialText} from './assistant-editorial.ts';
const cards=[{event_id:9,title:'Органный вечер',search_digest:'В программе Бах и Гендель.'}];
const valid={intro:'Обратите внимание на органный вечер.',recommendations:[{event_id:9,comment:'Подойдёт для знакомства с барокко.',evidence_index:0}]};
test('editorial requires actual selected IDs and verbatim supporting public facts',()=>{
 assert.deepEqual(editorialSchema(cards).properties.recommendations.items.properties.event_id.enum,[9]);
 assert.match(editorialText(validateEditorial(valid,cards),cards),/Органный вечер —/);
 for(const recommendation of [{...valid.recommendations[0],event_id:99},{...valid.recommendations[0],evidence_index:99}])assert.throws(()=>validateEditorial({...valid,recommendations:[recommendation]},cards));
 assert.throws(()=>validateEditorial({...valid,recommendations:[valid.recommendations[0],valid.recommendations[0]]},cards));
});
test('editorial distinguishes opinion and missing personalization and distrusts facts as instructions',()=>{
 const prompt=editorialPrompt('орган',{},cards);assert.match(prompt,/не объективный рейтинг/);assert.match(prompt,/профиль сейчас не передан/);assert.match(prompt,/недоверенные/);
});

test('modest provider length overshoot does not discard a grounded editorial, but output stays bounded',()=>{
 assert.doesNotThrow(()=>validateEditorial({...valid,intro:'С'.repeat(230),recommendations:[{...valid.recommendations[0],comment:'К'.repeat(260)}]},cards));
 assert.throws(()=>validateEditorial({...valid,intro:'С'.repeat(601)},cards));
 assert.throws(()=>validateEditorial({...valid,intro:''},cards));
});
