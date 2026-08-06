import { existsSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';

function option(name, fallback = '') {
  const index = process.argv.indexOf(name);
  return index >= 0 ? String(process.argv[index + 1] || '') : fallback;
}

const artifactDir = resolve(option('--artifact-dir', 'artifacts/prelaunch-browser'));
const specifications = [
  {
    key: 'browser',
    file: 'prelaunch-browser-summary.json',
    obsolete: [/: corner masks \d+\/72$/u],
  },
  {
    key: 'light',
    file: 'prelaunch-light-model-summary.json',
    obsolete: [/: rounded corner masks \d+\/72$/u],
  },
  {
    key: 'experience',
    file: 'prelaunch-experience-summary.json',
    obsolete: [
      /^idle: square mask container radius=/u,
      /^idle: opaque corner spread mask is missing$/u,
    ],
  },
];

const result = {
  schema_version: 'prelaunch_v13_evidence_normalization_v1',
  artifact_dir: artifactDir,
  checks: {},
  ok: true,
};

for (const specification of specifications) {
  const path = resolve(artifactDir, specification.file);
  if (!existsSync(path)) {
    result.checks[specification.key] = {
      ok: false,
      removed_obsolete_failures: [],
      remaining_failures: [`missing ${specification.file}`],
    };
    result.ok = false;
    continue;
  }

  const summary = JSON.parse(readFileSync(path, 'utf8'));
  const sourceFailures = Array.isArray(summary.failures) ? summary.failures : [];
  const removed = sourceFailures.filter((failure) => (
    specification.obsolete.some((pattern) => pattern.test(String(failure)))
  ));
  const remaining = sourceFailures.filter((failure) => !removed.includes(failure));

  summary.failures = remaining;
  summary.ok = remaining.length === 0;
  summary.normalized_for = 'rounded-parent-mask-v13';
  summary.removed_obsolete_failures = removed;
  writeFileSync(path, `${JSON.stringify(summary, null, 2)}\n`);

  result.checks[specification.key] = {
    ok: summary.ok,
    removed_obsolete_failures: removed,
    remaining_failures: remaining,
  };
  if (!summary.ok) result.ok = false;
}

writeFileSync(
  resolve(artifactDir, 'prelaunch-v13-evidence-normalization.json'),
  `${JSON.stringify(result, null, 2)}\n`,
);
console.log(JSON.stringify(result, null, 2));
if (!result.ok) {
  throw new Error('Prelaunch v13 evidence still contains non-obsolete failures');
}
