#!/usr/bin/env node

import { randomUUID } from 'node:crypto';
import { mkdir, writeFile, appendFile } from 'node:fs/promises';
import { dirname } from 'node:path';
import process from 'node:process';

const SMOKE_MARKER = '[lovekgd-penpot-api-smoke-v1]';
const DEFAULT_BASE_URL = 'https://design.penpot.app';
const DEFAULT_TEAM_HINT = 'Полюбить Калининград';
const DEFAULT_PROJECT_HINT = 'Design System';
const DEFAULT_FILE_NAME = '00 — LoveKGD API smoke test';
const DEFAULT_RESULT_PATH = 'artifacts/penpot-smoke/penpot-smoke-result.json';
const ROOT_FRAME_FALLBACK = '00000000-0000-0000-0000-000000000000';

function toKebabCase(key) {
  return key.replace(/([a-z0-9])([A-Z])/g, '$1-$2').toLowerCase();
}

function convertKeysToKebabCase(value) {
  if (Array.isArray(value)) return value.map(convertKeysToKebabCase);
  if (value && typeof value === 'object') {
    return Object.fromEntries(
      Object.entries(value).map(([key, child]) => [toKebabCase(key), convertKeysToKebabCase(child)]),
    );
  }
  return value;
}

function getAny(object, ...keys) {
  for (const key of keys) {
    if (object && Object.prototype.hasOwnProperty.call(object, key)) return object[key];
  }
  return undefined;
}

function normalizeName(value) {
  return String(value ?? '')
    .trim()
    .toLocaleLowerCase('ru-RU')
    .replace(/[\s_–—-]+/g, ' ')
    .replace(/[^\p{L}\p{N} ]/gu, '');
}

function chooseByHint(items, { explicitId, hint, kind, fallbackNames = [] }) {
  if (!Array.isArray(items) || items.length === 0) {
    throw new Error(`Penpot returned no ${kind} records accessible to this token.`);
  }

  if (explicitId) {
    const byId = items.find((item) => String(item.id) === String(explicitId));
    if (!byId) throw new Error(`Configured ${kind} id ${explicitId} is not accessible.`);
    return byId;
  }

  const normalizedHints = [hint, ...fallbackNames].filter(Boolean).map(normalizeName);
  const scored = items.map((item) => {
    const name = normalizeName(item.name);
    let score = 0;
    for (const [index, candidate] of normalizedHints.entries()) {
      if (!candidate) continue;
      if (name === candidate) score = Math.max(score, 100 - index);
      else if (name.includes(candidate) || candidate.includes(name)) score = Math.max(score, 70 - index);
    }
    if (/lovekgd|полюбить калининград|design system|дизайн система/u.test(name)) score += 20;
    if (getAny(item, 'isDefault', 'is-default')) score -= 10;
    return { item, score };
  });

  scored.sort((a, b) => b.score - a.score || String(a.item.name).localeCompare(String(b.item.name), 'ru'));
  const best = scored[0];
  const second = scored[1];

  if (best.score > 0 && (!second || best.score > second.score)) return best.item;

  const nonDefault = items.filter((item) => !getAny(item, 'isDefault', 'is-default'));
  if (nonDefault.length === 1) return nonDefault[0];
  if (items.length === 1) return items[0];

  const safeInventory = items.map((item) => `${item.name} (${item.id})`).join(', ');
  throw new Error(
    `Could not select one ${kind}. Set the corresponding PENPOT_*_ID variable. Accessible ${kind}: ${safeInventory}`,
  );
}

function shapeGeometry(x, y, width, height) {
  return {
    selrect: { x, y, width, height, x1: x, y1: y, x2: x + width, y2: y + height },
    points: [
      { x, y },
      { x: x + width, y },
      { x: x + width, y: y + height },
      { x, y: y + height },
    ],
    transform: { a: 1, b: 0, c: 0, d: 1, e: 0, f: 0 },
    transformInverse: { a: 1, b: 0, c: 0, d: 1, e: 0, f: 0 },
  };
}

function createFrameShape({ id, rootFrameId, x, y, width, height }) {
  return {
    id,
    type: 'frame',
    name: `${SMOKE_MARKER} Component review specimen`,
    x,
    y,
    width,
    height,
    parentId: rootFrameId,
    frameId: rootFrameId,
    shapes: [],
    fills: [{ fillColor: '#F7F2EA', fillOpacity: 1 }],
    strokes: [{ strokeColor: '#D9CCBD', strokeWidth: 1, strokeOpacity: 1, strokeStyle: 'solid', strokeAlignment: 'center' }],
    r1: 24,
    r2: 24,
    r3: 24,
    r4: 24,
    ...shapeGeometry(x, y, width, height),
  };
}

function createRectangleShape({ id, frameId, x, y, width, height }) {
  return {
    id,
    type: 'rect',
    name: '[ds:core.button.smoke] Primary button',
    x,
    y,
    width,
    height,
    parentId: frameId,
    frameId,
    fills: [{ fillColor: '#98401F', fillOpacity: 1 }],
    strokes: [],
    r1: 14,
    r2: 14,
    r3: 14,
    r4: 14,
    shadow: [
      {
        id: randomUUID(),
        style: 'drop-shadow',
        offsetX: 0,
        offsetY: 8,
        blur: 24,
        spread: 0,
        hidden: false,
        color: { color: '#482D19', opacity: 0.18 },
      },
    ],
    ...shapeGeometry(x, y, width, height),
  };
}

function createTextShape({ id, frameId, name, text, x, y, fontSize, fillColor, width, height, fontWeight = 'normal' }) {
  const calculatedWidth = width ?? Math.max(80, text.length * fontSize * 0.62);
  const calculatedHeight = height ?? fontSize * 1.5;
  return {
    id,
    type: 'text',
    name,
    x,
    y,
    width: calculatedWidth,
    height: calculatedHeight,
    parentId: frameId,
    frameId,
    content: {
      type: 'root',
      children: [
        {
          type: 'paragraph-set',
          children: [
            {
              type: 'paragraph',
              textAlign: 'left',
              children: [
                {
                  text,
                  fills: [{ fillColor, fillOpacity: 1 }],
                  fontSize: String(fontSize),
                  fontFamily: 'Work Sans',
                  fontWeight,
                  fontStyle: 'normal',
                  textDecoration: 'none',
                  letterSpacing: '0',
                  lineHeight: 1.25,
                },
              ],
            },
          ],
        },
      ],
    },
    fontSize: String(fontSize),
    fontFamily: 'Work Sans',
    fontWeight,
    fontStyle: 'normal',
    fills: [{ fillColor, fillOpacity: 1 }],
    verticalAlign: 'top',
    ...shapeGeometry(x, y, calculatedWidth, calculatedHeight),
  };
}

function fileData(file) {
  return getAny(file, 'data') ?? {};
}

function pageIndex(data) {
  return getAny(data, 'pagesIndex', 'pages-index') ?? {};
}

function getPageId(file) {
  const data = fileData(file);
  const pages = getAny(data, 'pages') ?? [];
  if (Array.isArray(pages) && pages.length) {
    const first = pages[0];
    return typeof first === 'string' ? first : first?.id;
  }
  const keys = Object.keys(pageIndex(data));
  return keys[0];
}

function getPage(file, pageId) {
  return pageIndex(fileData(file))[pageId];
}

function getObjects(page) {
  return getAny(page, 'objects') ?? {};
}

function findRootFrameId(page, pageId) {
  for (const [id, object] of Object.entries(getObjects(page))) {
    const parentId = getAny(object, 'parentId', 'parent-id');
    if (object?.type === 'frame' && !parentId) return id;
  }
  return pageId || ROOT_FRAME_FALLBACK;
}

function findSmokeFrame(page) {
  return Object.values(getObjects(page)).find(
    (object) => object?.type === 'frame' && String(object?.name ?? '').includes(SMOKE_MARKER),
  );
}

function directWorkspaceUrl({ baseUrl, teamId, fileId, pageId }) {
  const origin = new URL(baseUrl).origin;
  const params = new URLSearchParams({ 'team-id': teamId, 'file-id': fileId, 'page-id': pageId });
  return `${origin}/#/workspace?${params.toString()}`;
}

async function appendGithubFile(path, value) {
  if (!path) return;
  await appendFile(path, `${value}\n`, 'utf8');
}

class PenpotApi {
  constructor({ baseUrl, token }) {
    this.baseUrl = baseUrl.replace(/\/$/, '');
    this.token = token;
  }

  async rpc(command, body = {}) {
    const response = await fetch(`${this.baseUrl}/api/rpc/command/${command}`, {
      method: 'POST',
      headers: {
        Authorization: `Token ${this.token}`,
        'Content-Type': 'application/json',
        Accept: 'application/json',
      },
      body: JSON.stringify(convertKeysToKebabCase(body)),
      signal: AbortSignal.timeout(30_000),
    });

    const raw = await response.text();
    let payload = null;
    if (raw) {
      try {
        payload = JSON.parse(raw);
      } catch {
        payload = raw;
      }
    }

    if (!response.ok) {
      const detail = typeof payload === 'string' ? payload.slice(0, 500) : JSON.stringify(payload)?.slice(0, 500);
      throw new Error(`Penpot ${command} failed with HTTP ${response.status}: ${detail || 'empty response'}`);
    }
    return payload;
  }
}

async function selfTest() {
  const converted = convertKeysToKebabCase({ sessionId: 'x', changes: [{ pageId: 'p', obj: { fillColor: '#fff' } }] });
  if (converted['session-id'] !== 'x' || converted.changes[0]['page-id'] !== 'p' || converted.changes[0].obj['fill-color'] !== '#fff') {
    throw new Error('camelCase → kebab-case conversion failed');
  }
  const geometry = shapeGeometry(10, 20, 30, 40);
  if (geometry.selrect.x2 !== 40 || geometry.selrect.y2 !== 60) throw new Error('geometry self-test failed');
  const url = directWorkspaceUrl({ baseUrl: DEFAULT_BASE_URL, teamId: 't', fileId: 'f', pageId: 'p' });
  if (!url.includes('team-id=t') || !url.includes('file-id=f') || !url.includes('page-id=p')) {
    throw new Error('workspace URL self-test failed');
  }
  console.log('Self-test PASS');
}

async function main() {
  if (process.argv.includes('--self-test')) {
    await selfTest();
    return;
  }

  const token = process.env.PENPOT_INTEGRATION_TOKEN;
  if (!token) throw new Error('PENPOT_INTEGRATION_TOKEN is missing.');

  const baseUrl = process.env.PENPOT_BASE_URL || DEFAULT_BASE_URL;
  const resultPath = process.env.PENPOT_RESULT_PATH || DEFAULT_RESULT_PATH;
  const api = new PenpotApi({ baseUrl, token });

  await api.rpc('get-profile');
  const teams = await api.rpc('get-teams');
  const team = chooseByHint(teams, {
    explicitId: process.env.PENPOT_TEAM_ID,
    hint: process.env.PENPOT_TEAM_NAME || DEFAULT_TEAM_HINT,
    kind: 'team',
    fallbackNames: ['LoveKGD', 'Love KGD', 'Полюбить Калининград'],
  });

  const projects = await api.rpc('get-projects', { teamId: team.id });
  const project = chooseByHint(projects, {
    explicitId: process.env.PENPOT_PROJECT_ID,
    hint: process.env.PENPOT_PROJECT_NAME || DEFAULT_PROJECT_HINT,
    kind: 'project',
    fallbackNames: ['Design System', 'Дизайн-система', 'Дизайн система'],
  });

  const fileName = process.env.PENPOT_SMOKE_FILE_NAME || DEFAULT_FILE_NAME;
  const files = await api.rpc('get-project-files', { projectId: project.id });
  let file = files.find((candidate) => normalizeName(candidate.name) === normalizeName(fileName));
  let fileCreated = false;
  if (!file) {
    file = await api.rpc('create-file', { projectId: project.id, name: fileName, isShared: false });
    fileCreated = true;
  }

  file = await api.rpc('get-file', { id: file.id });
  let pageId = getPageId(file);
  if (!pageId) throw new Error('The Penpot file has no page id after creation.');
  let page = getPage(file, pageId);
  if (!page) {
    page = await api.rpc('get-page', { fileId: file.id, pageId });
  }
  if (!page) throw new Error(`Unable to read Penpot page ${pageId}.`);

  let smokeFrame = findSmokeFrame(page);
  let buttonId = null;
  let labelId = null;
  let commentThread = null;
  let visualCreated = false;

  if (!smokeFrame) {
    const rootFrameId = findRootFrameId(page, pageId);
    const frameId = randomUUID();
    buttonId = randomUUID();
    labelId = randomUUID();
    const headingId = randomUUID();
    const descriptionId = randomUUID();
    const frameX = 120;
    const frameY = 120;
    const buttonX = frameX + 80;
    const buttonY = frameY + 235;
    const buttonWidth = 292;
    const buttonHeight = 64;

    const changes = [
      {
        type: 'add-obj',
        id: frameId,
        pageId,
        frameId: rootFrameId,
        obj: createFrameShape({ id: frameId, rootFrameId, x: frameX, y: frameY, width: 760, height: 430 }),
      },
      {
        type: 'add-obj',
        id: headingId,
        pageId,
        frameId,
        obj: createTextShape({
          id: headingId,
          frameId,
          name: 'Smoke-test heading',
          text: 'LoveKGD Design System',
          x: frameX + 80,
          y: frameY + 72,
          fontSize: 32,
          fillColor: '#2B211B',
          fontWeight: '600',
        }),
      },
      {
        type: 'add-obj',
        id: descriptionId,
        pageId,
        frameId,
        obj: createTextShape({
          id: descriptionId,
          frameId,
          name: 'Smoke-test instruction',
          text: 'API smoke test: оставьте комментарий поверх кнопки.',
          x: frameX + 80,
          y: frameY + 128,
          fontSize: 18,
          fillColor: '#6A564A',
          width: 560,
        }),
      },
      {
        type: 'add-obj',
        id: buttonId,
        pageId,
        frameId,
        obj: createRectangleShape({
          id: buttonId,
          frameId,
          x: buttonX,
          y: buttonY,
          width: buttonWidth,
          height: buttonHeight,
        }),
      },
      {
        type: 'add-obj',
        id: labelId,
        pageId,
        frameId,
        obj: createTextShape({
          id: labelId,
          frameId,
          name: 'Primary button label',
          text: 'Оставить комментарий',
          x: buttonX + 34,
          y: buttonY + 19,
          fontSize: 18,
          fillColor: '#FFFFFF',
          fontWeight: '600',
        }),
      },
    ];

    const freshFile = await api.rpc('get-file', { id: file.id });
    await api.rpc('update-file', {
      id: file.id,
      sessionId: randomUUID(),
      revn: getAny(freshFile, 'revn') ?? 0,
      vern: 0,
      changes,
    });

    visualCreated = true;
    smokeFrame = { id: frameId, name: `${SMOKE_MARKER} Component review specimen` };
  } else {
    const objects = getObjects(page);
    buttonId = Object.values(objects).find((object) => object?.name === '[ds:core.button.smoke] Primary button')?.id ?? null;
    labelId = Object.values(objects).find((object) => object?.name === 'Primary button label')?.id ?? null;
  }

  file = await api.rpc('get-file', { id: file.id });
  pageId = getPageId(file) || pageId;
  page = getPage(file, pageId) || page;
  const verifiedFrame = findSmokeFrame(page);
  if (!verifiedFrame) throw new Error('Penpot accepted update-file but the smoke-test frame is absent on reread.');

  const verifiedObjects = getObjects(page);
  const verifiedButton = Object.values(verifiedObjects).find(
    (object) => object?.name === '[ds:core.button.smoke] Primary button',
  );
  buttonId = verifiedButton?.id ?? buttonId;
  labelId = Object.values(verifiedObjects).find((object) => object?.name === 'Primary button label')?.id ?? labelId;

  const threads = (await api.rpc('get-comment-threads', { fileId: file.id })) ?? [];
  for (const thread of Array.isArray(threads) ? threads : []) {
    const inlineContent = String(getAny(thread, 'content', 'message', 'text') ?? '');
    if (inlineContent.includes(SMOKE_MARKER)) {
      commentThread = thread;
      break;
    }
    const threadId = getAny(thread, 'id');
    if (!threadId) continue;
    try {
      const comments = (await api.rpc('get-comments', { threadId })) ?? [];
      if (Array.isArray(comments) && comments.some((comment) => String(getAny(comment, 'content', 'message', 'text') ?? '').includes(SMOKE_MARKER))) {
        commentThread = thread;
        break;
      }
    } catch {
      // A thread may not be readable through this endpoint version; the dedicated
      // smoke file remains safe, so absence of a marker is handled below.
    }
  }

  if (!commentThread) {
    const buttonX = Number(getAny(verifiedButton, 'x') ?? 200);
    const buttonY = Number(getAny(verifiedButton, 'y') ?? 355);
    const buttonWidth = Number(getAny(verifiedButton, 'width') ?? 292);
    commentThread = await api.rpc('create-comment-thread', {
      fileId: file.id,
      pageId,
      frameId: verifiedFrame.id,
      position: { x: buttonX + buttonWidth - 18, y: buttonY + 18 },
      content:
        `${SMOKE_MARKER}\n` +
        'Оставьте ответ или новый комментарий поверх кнопки. Следующий smoke test заберёт открытые комментарии и соберёт короткий prompt для новой сессии ChatGPT.\n' +
        'dsElementId: core.button.smoke',
      mentions: [],
    });
  }

  const workspaceUrl = directWorkspaceUrl({ baseUrl, teamId: team.id, fileId: file.id, pageId });
  const result = {
    status: 'PASS',
    mode: 'personal-access-token/internal-api',
    marker: SMOKE_MARKER,
    fileCreated,
    visualCreated,
    team: { id: team.id, name: team.name },
    project: { id: project.id, name: project.name },
    file: { id: file.id, name: file.name, revision: getAny(file, 'revn') ?? null },
    page: { id: pageId, name: getAny(page, 'name') ?? null },
    objects: {
      frameId: verifiedFrame.id,
      buttonId,
      labelId,
      commentThreadId: commentThread?.id ?? null,
      dsElementId: 'core.button.smoke',
    },
    workspaceUrl,
    generatedAt: new Date().toISOString(),
  };

  await mkdir(dirname(resultPath), { recursive: true });
  await writeFile(resultPath, `${JSON.stringify(result, null, 2)}\n`, 'utf8');

  await appendGithubFile(process.env.GITHUB_OUTPUT, `workspace_url=${workspaceUrl}`);
  await appendGithubFile(process.env.GITHUB_OUTPUT, `file_id=${file.id}`);
  await appendGithubFile(process.env.GITHUB_OUTPUT, `page_id=${pageId}`);
  await appendGithubFile(process.env.GITHUB_OUTPUT, `visual_created=${visualCreated}`);
  if (process.env.GITHUB_STEP_SUMMARY) {
    await appendGithubFile(process.env.GITHUB_STEP_SUMMARY, '## Penpot API smoke test — PASS');
    await appendGithubFile(process.env.GITHUB_STEP_SUMMARY, '');
    await appendGithubFile(process.env.GITHUB_STEP_SUMMARY, `- Team: **${team.name}**`);
    await appendGithubFile(process.env.GITHUB_STEP_SUMMARY, `- Project: **${project.name}**`);
    await appendGithubFile(process.env.GITHUB_STEP_SUMMARY, `- File: **${file.name}**`);
    await appendGithubFile(process.env.GITHUB_STEP_SUMMARY, `- Revision: \`${result.file.revision}\``);
    await appendGithubFile(process.env.GITHUB_STEP_SUMMARY, `- Created now: \`${visualCreated}\``);
    await appendGithubFile(process.env.GITHUB_STEP_SUMMARY, `- [Open the exact Penpot page](${workspaceUrl})`);
  }

  console.log(`PENPOT_SMOKE_PASS ${JSON.stringify({ team: team.name, project: project.name, file: file.name, pageId, visualCreated })}`);
  console.log(`PENPOT_WORKSPACE_URL ${workspaceUrl}`);
}

main().catch((error) => {
  const message = String(error?.stack || error?.message || error).replaceAll(process.env.PENPOT_INTEGRATION_TOKEN || '__missing__', '***');
  console.error(message);
  process.exitCode = 1;
});
