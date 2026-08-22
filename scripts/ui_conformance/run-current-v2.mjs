#!/usr/bin/env node
import { copyFileSync, existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { basename, dirname, isAbsolute, join, resolve } from 'node:path';
import { performance } from 'node:perf_hooks';
import { fileURLToPath } from 'node:url';
import { spawnSync } from 'node:child_process';
import { buildInventory } from './inventory-event-card-chips.mjs';
import { assertImmutableSha } from './immutable-checkout.mjs';

const scriptRoot = dirname(fileURLToPath(import.meta.url));
const readJson = (path) => JSON.parse(readFileSync(resolve(path), 'utf8'));

function parse(argv) {
  const [command = 'help', ...rest] = argv;
  const values = {};
  for (let index = 0; index < rest.length; index += 1) {
    const key = rest[index]; const value = rest[index + 1];
    if (!key.startsWith('--') || !value || value.startsWith('--')) throw new Error(`Invalid argument near ${key || '<end>'}`);
    values[key.slice(2)] = value; index += 1;
  }
  return { command, values };
}

function need(values, key) {
  if (!values[key]) throw new Error(`--${key} is required`);
  return values[key];
}

function run(command, argv, { allowed = [0] } = {}) {
  const result = spawnSync(command, argv, { encoding: 'utf8', env: process.env, maxBuffer: 20 * 1024 * 1024 });
  if (!allowed.includes(result.status)) throw new Error(`${command} ${argv.join(' ')} failed (${result.status}):\n${result.stderr.slice(-5000)}\n${result.stdout.slice(-5000)}`);
  return result;
}

function pathFrom(value, base) {
  return isAbsolute(value) ? value : resolve(base, value);
}

export function runCase(values) {
  for (const key of ['case', 'resolved', 'design-cli', 'design-sha', 'astro-source-site', 'astro-sha', 'tooling-root', 'tooling-sha', 'corpus-root', 'font-manifest', 'node-modules', 'workspace-root', 'artifacts-root', 'penpot', 'penpot-receipt', 'penpot-facts', 'agent-review']) need(values, key);
  assertImmutableSha(values['design-sha'], 'Design SHA');
  assertImmutableSha(values['astro-sha'], 'Astro source SHA');
  assertImmutableSha(values['tooling-sha'], 'Events tooling SHA');
  const caseRow = readJson(values.case);
  const runId = values['run-id'] || caseRow.case_id;
  if (runId !== caseRow.case_id) throw new Error('run-id must equal canonical case_id');
  const workspace = resolve(values['workspace-root'], caseRow.case_id);
  const harness = join(workspace, 'harness'); const capture = join(workspace, 'capture'); const tuple = join(workspace, 'actual-tuple.json');
  const artifactsRoot = resolve(values['artifacts-root']); const runDir = join(artifactsRoot, runId);
  mkdirSync(workspace, { recursive: true }); mkdirSync(artifactsRoot, { recursive: true });
  if (existsSync(runDir)) throw new Error(`Refusing to reuse run directory: ${runDir}`);
  const started = performance.now();
  run(process.execPath, [values['design-cli'], 'validate-case', '--case', values.case]);
  run(process.execPath, [values['design-cli'], 'init-run', '--artifacts-root', artifactsRoot, '--run-id', runId, '--retention-class', 'gha-3d']);
  run(process.execPath, [join(scriptRoot, 'materialize-case.mjs'),
    '--resolved', values.resolved,
    '--astro-source-site', values['astro-source-site'], '--astro-source-sha', values['astro-sha'],
    '--tooling-root', values['tooling-root'], '--tooling-sha', values['tooling-sha'],
    '--harness', harness, '--corpus-root', values['corpus-root'], '--font-manifest', values['font-manifest'], '--node-modules', values['node-modules'],
  ]);
  run(process.execPath, [join(scriptRoot, 'capture-case.mjs'), '--resolved', values.resolved, '--harness', harness, '--output', capture, '--node-modules', values['node-modules']]);
  run(process.execPath, [join(scriptRoot, 'build-actual-tuple.mjs'),
    '--case', values.case, '--resolved', values.resolved, '--capture', capture,
    '--font-manifest', values['font-manifest'], '--materialization-receipt', join(harness, 'specimen-materialization-receipt.json'),
    '--design-sha', values['design-sha'], '--penpot', values.penpot, '--penpot-receipt', values['penpot-receipt'], '--output', tuple,
  ]);
  copyFileSync(tuple, join(runDir, 'actual-tuple.json'));
  copyFileSync(join(harness, 'specimen-materialization-receipt.json'), join(runDir, 'specimen-materialization-receipt.json'));
  copyFileSync(join(capture, 'astro-capture-receipt.json'), join(runDir, 'astro-capture-receipt.json'));
  run(process.execPath, [values['design-cli'], 'compare', '--case', values.case, '--actual-tuple', tuple, '--astro', join(capture, 'astro.png'), '--astro-facts', join(capture, 'astro-facts.json'), '--run-dir', runDir, '--penpot', values.penpot, '--penpot-facts', values['penpot-facts']], { allowed: [0, 2] });
  copyFileSync(values['agent-review'], join(runDir, 'agent-review.json'));
  run(process.execPath, [values['design-cli'], 'finalize', '--case', values.case, '--run-dir', runDir, '--actual-tuple', tuple, '--run-id', runId]);
  const finalReceipt = readJson(join(runDir, 'final-receipt.json'));
  return { case_id: caseRow.case_id, run_dir: runDir, final_status: finalReceipt.final?.status ?? null, elapsed_ms: Math.round(performance.now() - started) };
}

export function runBatch(values) {
  const manifestPath = resolve(need(values, 'batch-manifest'));
  const manifest = readJson(manifestPath);
  if (manifest.batch_id !== 'event-card-large-current-v2' || !Array.isArray(manifest.cases) || manifest.cases.length !== 7) {
    throw new Error('run-batch accepts only the exact seven-case event-card-large-current-v2 manifest');
  }
  const base = dirname(manifestPath); const results = []; let failed = false;
  for (const row of manifest.cases) {
    try {
      results.push(runCase({ ...values,
        case: pathFrom(row.case, base), resolved: pathFrom(row.resolved, base),
        penpot: pathFrom(row.penpot, base), 'penpot-receipt': pathFrom(row.penpot_receipt, base),
        'penpot-facts': pathFrom(row.penpot_facts, base), 'agent-review': pathFrom(row.agent_review, base),
        'run-id': row.case_id,
      }));
    } catch (error) {
      failed = true; results.push({ case_id: row.case_id, error: error instanceof Error ? error.message : String(error) });
    }
  }
  const output = resolve(values['artifacts-root'], `${manifest.batch_id}.run-manifest.json`);
  writeFileSync(output, `${JSON.stringify({ schema_version: 'event_card_large_current_v2_batch_run.v1', batch_id: manifest.batch_id, case_count: manifest.cases.length, results }, null, 2)}\n`);
  if (failed) throw new Error(`One or more current-v2 cases failed; inspect ${output}`);
  return { batch_id: manifest.batch_id, output, results };
}

const help = `run-current-v2 commands:
  chip-inventory --astro-source-site <site> --astro-source-sha <40hex> --tooling-root <repo> --tooling-sha <40hex> --corpus-root <v1> --semantic-census <json> --node-modules <dir> --harness <dir> --output <json>
  run-case --case <json> --resolved <json> --design-cli <ui-conformance.mjs> --design-sha <40hex> --astro-source-site <site> --astro-sha <40hex> --tooling-root <repo> --tooling-sha <40hex> --corpus-root <v1> --font-manifest <json> --node-modules <dir> --workspace-root <dir> --artifacts-root <dir> --penpot <png> --penpot-receipt <json> --penpot-facts <json> --agent-review <json>
  run-batch --batch-manifest <seven-case-json> plus the shared run-case checkout/tooling/workspace arguments
`;

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const { command, values } = parse(process.argv.slice(2));
  try {
    const result = command === 'chip-inventory' ? buildInventory({ ...values, 'astro-source-sha': values['astro-source-sha'] || values['astro-sha'] })
      : command === 'run-case' ? runCase(values)
        : command === 'run-batch' ? runBatch(values)
          : null;
    if (!result) process.stdout.write(help); else process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
  } catch (error) {
    process.stderr.write(`${error instanceof Error ? error.stack : String(error)}\n`); process.exitCode = 1;
  }
}
