import { createHash } from 'node:crypto';
import { readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';

const sha=(value)=>createHash('sha256').update(value).digest('hex');
const lineOf=(text,offset)=>text.slice(0,offset).split('\n').length;
const compact=(value)=>String(value||'').replace(/\/\*[\s\S]*?\*\//gu,' ').replace(/\s+/gu,' ').trim();
export const normalizeCssText=(value)=>compact(value).replace(/\s*([:;,(){}])\s*/gu,'$1').toLowerCase();

function sourceId(path,content){return `behavior-source.${sha(`${path}\0${sha(content)}`).slice(0,16)}`;}
function legacyProbeId({source_id,offset,threshold=null,delta=null}){
  const key=threshold===null?`${source_id}\0${offset}`:`${source_id}\0${offset}\0${threshold}\0${delta}`;
  return `breakpoint.${sha(key).slice(0,16)}`;
}

function skipCommentOrString(text,index){
  if(text[index]==='/'&&text[index+1]==='*'){
    const end=text.indexOf('*/',index+2);return end<0?text.length:end+2;
  }
  const quote=text[index];if(quote!=="'"&&quote!=='"')return index;
  let cursor=index+1;while(cursor<text.length){if(text[cursor]==='\\'){cursor+=2;continue;}if(text[cursor]===quote)return cursor+1;cursor+=1;}return text.length;
}
function matchingBrace(text,open){
  let depth=1;for(let cursor=open+1;cursor<text.length;cursor+=1){
    const next=skipCommentOrString(text,cursor);if(next!==cursor){cursor=next-1;continue;}
    if(text[cursor]==='{')depth+=1;else if(text[cursor]==='}'&&--depth===0)return cursor;
  }return text.length-1;
}
function findOpenBrace(text,start){
  let parens=0;for(let cursor=start;cursor<text.length;cursor+=1){
    const next=skipCommentOrString(text,cursor);if(next!==cursor){cursor=next-1;continue;}
    if(text[cursor]==='(')parens+=1;else if(text[cursor]===')')parens=Math.max(0,parens-1);else if(text[cursor]==='{'&&parens===0)return cursor;else if(text[cursor]===';'&&parens===0)return -1;
  }return -1;
}

function parseDeclarations(body,baseOffset){
  const declarations=[];let start=0,parens=0;
  const emit=(end)=>{const raw=body.slice(start,end).trim();start=end+1;if(!raw)return;let colon=-1,depth=0;for(let i=0;i<raw.length;i+=1){const next=skipCommentOrString(raw,i);if(next!==i){i=next-1;continue;}if(raw[i]==='(')depth+=1;else if(raw[i]===')')depth=Math.max(0,depth-1);else if(raw[i]===':'&&depth===0){colon=i;break;}}if(colon<=0)return;const property=raw.slice(0,colon).trim().toLowerCase();const value=raw.slice(colon+1).trim();if(!/^--[\w-]+$|^[a-z][\w-]*$/u.test(property)||!value)return;declarations.push({property,value,important:/\s*!important\s*$/iu.test(value),offset:baseOffset+Math.max(0,body.indexOf(raw)),fingerprint:sha(`${property}\0${normalizeCssText(value)}`)});};
  for(let i=0;i<body.length;i+=1){const next=skipCommentOrString(body,i);if(next!==i){i=next-1;continue;}if(body[i]==='(')parens+=1;else if(body[i]===')')parens=Math.max(0,parens-1);else if(body[i]===';'&&parens===0)emit(i);}emit(body.length);return declarations;
}

function parseStyleRules(text,start,end,rows=[]){
  let cursor=start;
  while(cursor<end){
    while(cursor<end&&/[\s;]/u.test(text[cursor]))cursor+=1;
    if(cursor>=end)break;
    const next=skipCommentOrString(text,cursor);if(next!==cursor){cursor=next;continue;}
    const open=findOpenBrace(text,cursor);if(open<0||open>=end)break;const close=Math.min(matchingBrace(text,open),end);const prelude=compact(text.slice(cursor,open));
    if(prelude.startsWith('@')){
      if(!/^@(?:keyframes|-webkit-keyframes|font-face|page|property)\b/iu.test(prelude))parseStyleRules(text,open+1,close,rows);
    }else if(prelude){
      const declarations=parseDeclarations(text.slice(open+1,close),open+1);
      if(declarations.length){
        const selectors=prelude.split(',').map((value)=>compact(value)).filter(Boolean);
        rows.push({selector:prelude,selectors,declarations,offset:cursor,line:lineOf(text,cursor),fingerprint:sha(`${normalizeCssText(prelude)}\0${declarations.map((item)=>`${item.property}:${normalizeCssText(item.value)}`).join(';')}`)});
      }
    }
    cursor=close+1;
  }
  return rows;
}

export function parseConditionFeatures(query){
  const features=[];let ordinal=0;
  for(const match of String(query).matchAll(/\(\s*([\w-]+)\s*(?::\s*([^)]*?))?\s*\)/gu)){
    const name=match[1].toLowerCase();const value=(match[2]??'').trim();const numeric=value.match(/^(-?\d+(?:\.\d+)?)px$/iu);
    const axis=/width$/u.test(name)?'width':/height$/u.test(name)?'height':name==='prefers-reduced-motion'?'reduced-motion':name==='hover'?'hover':name==='pointer'?'pointer':'other';
    features.push({ordinal:ordinal++,name,value,axis,comparison:name.startsWith('min-')?'min':name.startsWith('max-')?'max':'equals',threshold_px:numeric?Number(numeric[1]):null,offset:match.index});
  }
  return features;
}

export function parseSourceAtRules({path,content}){
  const rows=[];let ordinal=0,mediaOrdinal=0,containerOrdinal=0;
  // Deliberately begin with the legacy token contract.  Astro source is not a
  // single JavaScript grammar (frontmatter, HTML and CSS coexist), so treating
  // every quote in the complete file as a JS/CSS string would skip valid style
  // blocks. Brace and declaration parsing becomes grammar-aware after the
  // exact legacy @-token offset has been identified.
  for(const token of content.matchAll(/@(media|container)\s*/giu)){
    const cursor=token.index;const kind=token[1].toLowerCase();const open=findOpenBrace(content,cursor+token[0].length);if(open<0)continue;const close=matchingBrace(content,open);const rawPrelude=compact(content.slice(cursor+token[0].length,open));
    let container_name=null,conditionQuery=rawPrelude;
    if(kind==='container'){
      const firstParen=rawPrelude.indexOf('(');if(firstParen>0){container_name=rawPrelude.slice(0,firstParen).trim()||null;conditionQuery=rawPrelude.slice(firstParen).trim();}
    }
    const features=parseConditionFeatures(conditionQuery);const rules=parseStyleRules(content,open+1,close);
    rows.push({kind,query:rawPrelude,condition_query:conditionQuery,raw_prelude:rawPrelude,container_name,features,offset:cursor,line:lineOf(content,cursor),ordinal:ordinal++,kind_ordinal:kind==='media'?mediaOrdinal++:containerOrdinal++,open_offset:open,close_offset:close,rules,affected_selectors:[...new Set(rules.flatMap((rule)=>rule.selectors))],affected_declarations:rules.flatMap((rule)=>rule.declarations.map((decl)=>({selector:rule.selector,property:decl.property,value:decl.value,important:decl.important,fingerprint:decl.fingerprint}))),at_rule_fingerprint:sha(`${path}\0${cursor}\0${kind}\0${normalizeCssText(rawPrelude)}\0${rules.map((rule)=>rule.fingerprint).join('\0')}`),rule_fingerprint:sha(rules.map((rule)=>rule.fingerprint).join('\0'))});
    // matchAll deliberately continues inside the block: nested occurrences are
    // part of the legacy source matrix identity and retain their exact offsets.
  }
  return rows;
}

export function enrichBreakpointMatrix({matrixRows,sourceRoot,sourceSha}){
  const root=resolve(sourceRoot);const cache=new Map();const indexed=new Map();
  for(const path of [...new Set(matrixRows.map((row)=>row.path))].sort()){
    const content=readFileSync(join(root,path),'utf8');const sid=sourceId(path,content);const atRules=parseSourceAtRules({path,content});cache.set(path,{content,sid,sha256:sha(content),atRules});
    for(const atRule of atRules){
      const numeric=atRule.features.filter((feature)=>feature.threshold_px!==null);
      if(!numeric.length)indexed.set(legacyProbeId({source_id:sid,offset:atRule.offset}),{atRule,target_feature:null,delta:null});
      for(const feature of numeric)for(const delta of [-1,0,1])indexed.set(legacyProbeId({source_id:sid,offset:atRule.offset,threshold:feature.threshold_px,delta}),{atRule,target_feature:feature,delta});
    }
  }
  return matrixRows.map((row)=>{
    const source=cache.get(row.path);const found=indexed.get(row.id);if(!source||!found)throw new Error(`Exact source at-rule not found for ${row.id}`);
    const {atRule,target_feature,delta}=found;
    if(atRule.kind!==row.kind||normalizeCssText(atRule.query)!==normalizeCssText(row.query)||atRule.line!==row.line)throw new Error(`Breakpoint source identity drift: ${row.id}`);
    return {...row,source_sha:sourceSha,source_sha256:source.sha256,source_offset:atRule.offset,at_rule_ordinal:atRule.ordinal,at_rule_kind_ordinal:atRule.kind_ordinal,at_rule_fingerprint:atRule.at_rule_fingerprint,rule_fingerprint:atRule.rule_fingerprint,container_name:atRule.container_name,condition_query:atRule.condition_query,condition_features:atRule.features,target_feature,target_feature_ordinal:target_feature?.ordinal??null,axis:target_feature?(atRule.kind==='container'?`container-${target_feature.axis}`:`viewport-${target_feature.axis}`):atRule.features.map((item)=>item.axis).join('+')||'boolean',probe_delta:delta,affected_selectors:atRule.affected_selectors,affected_declarations:atRule.affected_declarations,source_rules:atRule.rules.map((rule)=>({selector:rule.selector,selectors:rule.selectors,declarations:rule.declarations.map(({property,value,important,fingerprint})=>({property,value,important,fingerprint})),fingerprint:rule.fingerprint,line:rule.line,offset:rule.offset}))};
  });
}

export function loadAndEnrichBreakpointMatrix({matrixPath,sourceRoot,sourceSha}){
  const rows=readFileSync(resolve(matrixPath),'utf8').split('\n').filter(Boolean).map(JSON.parse);
  return enrichBreakpointMatrix({matrixRows:rows,sourceRoot,sourceSha});
}
