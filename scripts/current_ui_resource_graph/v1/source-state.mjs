import { createHash } from 'node:crypto';

const MAX_FACTS_PER_KIND = 96;
const SAFE_LITERAL_LENGTH = 80;

function hash(value) {
  return createHash('sha256').update(String(value)).digest('hex');
}

function line(node) { return node?.loc?.start?.line ?? null; }

function identifierNames(node, result = new Set()) {
  if (!node || typeof node !== 'object') return result;
  if (node.type === 'Identifier') result.add(node.name);
  for (const [key, value] of Object.entries(node)) {
    if (['loc', 'start', 'end', 'leadingComments', 'trailingComments', 'innerComments'].includes(key)) continue;
    if (Array.isArray(value)) for (const child of value) identifierNames(child, result);
    else if (value && typeof value === 'object') identifierNames(value, result);
  }
  return result;
}

function boundedString(value) {
  const text = String(value);
  if (text.length <= SAFE_LITERAL_LENGTH && !/(?:https?:\/\/|bearer|authorization|token=|secret|password)/iu.test(text)) return text;
  return { redacted: true, sha256: hash(text), length: text.length };
}

function literalValue(node) {
  if (!node) return undefined;
  if (node.type === 'StringLiteral') return boundedString(node.value);
  if (node.type === 'NumericLiteral' || node.type === 'BooleanLiteral') return node.value;
  if (node.type === 'NullLiteral') return null;
  if (node.type === 'TemplateLiteral' && node.expressions.length === 0) return boundedString(node.quasis[0]?.value?.cooked || '');
  return undefined;
}

function typeSummary(node) {
  if (!node) return 'unknown';
  const name = node.type.replace(/^TS/u, '').replace(/Keyword$/u, '').replace(/Type$/u, '').toLowerCase();
  if (node.type === 'TSLiteralType') return { kind: 'literal', value: literalValue(node.literal) };
  if (node.type === 'TSUnionType') return { kind: 'union', members: node.types.slice(0, 32).map(typeSummary) };
  if (node.type === 'TSArrayType') return { kind: 'array', element: typeSummary(node.elementType) };
  if (node.type === 'TSTypeReference') return { kind: 'reference', name: [...identifierNames(node.typeName)].sort().join('.') || 'unknown' };
  return name;
}

function unionLiterals(summary, result = []) {
  if (!summary || typeof summary !== 'object') return result;
  if (summary.kind === 'literal' && summary.value !== undefined) result.push(summary.value);
  for (const member of summary.members || []) unionLiterals(member, result);
  return result;
}

function propertyName(node) {
  if (node?.key?.type === 'Identifier') return node.key.name;
  if (node?.key?.type === 'StringLiteral') return node.key.value;
  return null;
}

function walk(node, visit) {
  if (!node || typeof node !== 'object') return;
  visit(node);
  for (const [key, value] of Object.entries(node)) {
    if (['loc', 'start', 'end', 'leadingComments', 'trailingComments', 'innerComments'].includes(key)) continue;
    if (Array.isArray(value)) for (const child of value) walk(child, visit);
    else if (value && typeof value === 'object') walk(value, visit);
  }
}

function patternDefaults(node, result = new Map()) {
  if (!node) return result;
  if (node.type === 'ObjectPattern') for (const property of node.properties) {
    if (property.type !== 'ObjectProperty') continue;
    const name = propertyName(property);
    if (!name) continue;
    const value = property.value?.type === 'AssignmentPattern' ? literalValue(property.value.right) : undefined;
    result.set(name, { default: value, line: line(property) });
  }
  return result;
}

function pushBounded(target, fact) {
  if (target.length < MAX_FACTS_PER_KIND) target.push(fact);
}

function conditionFact(node) {
  const test = node.test || node.discriminant || node.left;
  return {
    kind: node.type,
    line: line(node),
    identifiers: [...identifierNames(test)].sort().slice(0, 24),
  };
}

export function extractStateAwareFacts(code, parser, { sourceKind = 'module' } = {}) {
  const empty = {
    parser: '@babel/parser', parser_status: 'not_attempted', props: [], branches: [], derived_state: [],
    flags: [], enums: [], responsive_contexts: [], state_attributes: [], interaction_attributes: [], media_rules: [],
  };
  if (!code.trim()) return { ...empty, parser_status: 'empty' };
  let ast;
  try {
    ast = parser.parse(code, {
      sourceType: sourceKind, errorRecovery: false, allowAwaitOutsideFunction: true,
      plugins: ['typescript', 'jsx', 'topLevelAwait', 'importAttributes'],
    });
  } catch (error) {
    return { ...empty, parser_status: 'parse_failed', reason_sha256: hash(error.message) };
  }
  const props = new Map(); const defaults = new Map(); const enums = new Map();
  const branches = []; const derived = []; const flags = new Set();
  walk(ast, (node) => {
    if (node.type !== 'TSEnumDeclaration' || !node.id?.name) return;
    const members = (node.members || []).slice(0, 32).map((member) => ({
      name: member.id?.name || member.id?.value || propertyName(member), value: literalValue(member.initializer), line: line(member),
    })).filter((member) => member.name);
    enums.set(node.id.name, { name: node.id.name, members, line: line(node) });
  });
  const collectProps = (members) => {
    for (const member of members || []) {
      if (member.type !== 'TSPropertySignature') continue;
      const name = propertyName(member); if (!name) continue;
      const summary = typeSummary(member.typeAnnotation?.typeAnnotation);
      props.set(name, {
        name, optional: Boolean(member.optional), required: !member.optional, type: summary,
        allowed_literals: unionLiterals(summary).slice(0, 32), line: line(member),
      });
    }
  };
  walk(ast, (node) => {
    if (node.type === 'TSInterfaceDeclaration' && node.id?.name === 'Props') {
      collectProps(node.body?.body);
    }
    if (node.type === 'TSTypeAliasDeclaration' && node.id?.name === 'Props' && node.typeAnnotation?.type === 'TSTypeLiteral') collectProps(node.typeAnnotation.members);
    if (node.type === 'VariableDeclarator' && node.init?.type === 'MemberExpression' &&
        node.init.object?.name === 'Astro' && node.init.property?.name === 'props') {
      for (const [name, value] of patternDefaults(node.id)) defaults.set(name, value);
    }
    if (['IfStatement', 'ConditionalExpression', 'SwitchStatement', 'LogicalExpression'].includes(node.type)) {
      pushBounded(branches, conditionFact(node));
    }
    if (node.type === 'VariableDeclarator' && node.id?.type === 'Identifier' &&
        /(?:state|status|mode|variant|layout|kind|open|hidden|expanded|disabled|fallback|crop|media|show|allow|has|is[A-Z_])/u.test(node.id.name) && node.init) {
      pushBounded(derived, { name: node.id.name, line: line(node), expression_kind: node.init.type, dependencies: [...identifierNames(node.init)].sort().slice(0, 24) });
    }
    if (node.type === 'Identifier' && /(?:experiment|featureFlag|flag|enabled|enable[A-Z_]|PUBLIC_)/iu.test(node.name)) flags.add(node.name);
  });
  for (const [name, value] of defaults) {
    const current = props.get(name) || { name, optional: true, required: false, type: 'inferred', allowed_literals: [], line: value.line };
    current.default = value.default === undefined ? { observed: false } : { observed: true, value: value.default };
    props.set(name, current);
  }
  for (const prop of props.values()) if (!('default' in prop)) prop.default = { observed: false };
  for (const prop of props.values()) {
    if (prop.type?.kind !== 'reference' || !enums.has(prop.type.name)) continue;
    prop.allowed_literals = enums.get(prop.type.name).members.map((member) => member.value).filter((value) => value !== undefined);
  }
  return {
    ...empty, parser_status: 'parsed', props: [...props.values()].sort((a, b) => a.name.localeCompare(b.name)),
    branches: branches.sort((a, b) => (a.line ?? 0) - (b.line ?? 0) || a.kind.localeCompare(b.kind)),
    derived_state: derived.sort((a, b) => a.name.localeCompare(b.name) || (a.line ?? 0) - (b.line ?? 0)),
    flags: [...flags].sort().slice(0, MAX_FACTS_PER_KIND),
    enums: [...enums.values()].sort((a, b) => a.name.localeCompare(b.name)),
  };
}

export function extractAstroStateFacts(source, parsedFacts, ast, parser = null) {
  const stateAttributes = []; const interaction = [];
  const responsive = []; const mediaRules = [];
  const attrPattern = /\b(data-[a-z0-9-]+|aria-(?:expanded|hidden|disabled)|hidden|open|disabled)\b/giu;
  for (const match of source.matchAll(attrPattern)) {
    const name = match[1].toLowerCase();
    const fact = { name, line: source.slice(0, match.index).split('\n').length };
    if (name.startsWith('data-')) pushBounded(stateAttributes, fact); else pushBounded(interaction, fact);
  }
  const queryPattern = /@(media|container)\s*([^\{]{1,240})\{/giu;
  for (const match of source.matchAll(queryPattern)) pushBounded(responsive, { kind: match[1].toLowerCase(), query: match[2].trim(), line: source.slice(0, match.index).split('\n').length });
  const mediaPattern = /\b(object-fit|aspect-ratio|object-position|background-image)\s*:\s*([^;\}]{1,120})/giu;
  for (const match of source.matchAll(mediaPattern)) pushBounded(mediaRules, { property: match[1].toLowerCase(), value: boundedString(match[2].trim()), line: source.slice(0, match.index).split('\n').length });
  const cssOverrides = [];
  walk(ast, (node) => {
    for (const attribute of node.attributes || []) {
      if (attribute.name === 'style' || attribute.name === 'class:list') pushBounded(cssOverrides, { kind: attribute.name, line: node.position?.start?.line ?? null });
    }
  });
  const scriptFacts = [];
  if (parser) {
    const scriptPattern = /<script([^>]*)>([\s\S]*?)<\/script>/giu;
    let scriptIndex = 0;
    for (const match of source.matchAll(scriptPattern)) {
      if (/type\s*=\s*["']application\/(?:ld\+)?json["']/iu.test(match[1])) continue;
      const facts = extractStateAwareFacts(match[2], parser);
      const sourceLineOffset = source.slice(0, match.index).split('\n').length;
      scriptFacts.push({ index: scriptIndex, source_line_offset: sourceLineOffset, parser_status: facts.parser_status, reason_sha256: facts.reason_sha256 || null });
      for (const fact of facts.branches) pushBounded(parsedFacts.branches, { ...fact, source_scope: 'inline_script', script_index: scriptIndex, source_line: fact.line === null ? null : sourceLineOffset + fact.line });
      for (const fact of facts.derived_state) pushBounded(parsedFacts.derived_state, { ...fact, source_scope: 'inline_script', script_index: scriptIndex, source_line: fact.line === null ? null : sourceLineOffset + fact.line });
      parsedFacts.flags = [...new Set([...parsedFacts.flags, ...facts.flags])].sort().slice(0, MAX_FACTS_PER_KIND);
      scriptIndex += 1;
    }
  }
  const inlineScriptFailed = scriptFacts.some((item) => item.parser_status === 'parse_failed');
  return {
    ...parsedFacts,
    parser_status: parsedFacts.parser_status === 'parse_failed' || inlineScriptFailed ? 'parse_failed' : scriptFacts.length ? 'parsed' : parsedFacts.parser_status,
    responsive_contexts: responsive,
    state_attributes: [...new Map(stateAttributes.map((item) => [`${item.name}:${item.line}`, item])).values()],
    interaction_attributes: [...new Map(interaction.map((item) => [`${item.name}:${item.line}`, item])).values()],
    media_rules: mediaRules,
    local_style_overrides: cssOverrides,
    inline_script_parsers: scriptFacts,
    extraction_limits: { max_facts_per_kind: MAX_FACTS_PER_KIND, max_literal_length: SAFE_LITERAL_LENGTH },
  };
}

export function inlineScriptImports(source, esm) {
  const imports = [];
  const pattern = /<script(?:\s[^>]*)?>([\s\S]*?)<\/script>/giu;
  for (const match of source.matchAll(pattern)) {
    let parsed = [];
    try { parsed = esm.parse(match[1])[0]; } catch { continue; }
    for (const entry of parsed) {
      const specifier = match[1].slice(entry.s, entry.e);
      if (specifier) imports.push(specifier);
    }
  }
  return [...new Set(imports)].sort();
}
