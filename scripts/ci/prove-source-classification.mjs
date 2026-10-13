#!/usr/bin/env node

import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { existsSync, mkdirSync, readFileSync, realpathSync, writeFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import path from 'node:path';
import process from 'node:process';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';

// Only public local commands, synthetic data and an empty child environment.
// No production-module imports, network operations, sessions or remote writes.
const repoRoot = fileURLToPath(new URL('../../', import.meta.url));
const { values } = parseArgs({
  options: {
    'out-dir': { type: 'string' },
    'cli-bin': { type: 'string', default: path.join(repoRoot, 'bin/tiangong-lca.js') },
    expect: { type: 'string', default: 'repaired' },
  },
});
assert.ok(values['out-dir'], 'Pass --out-dir with a new evidence directory.');
assert.ok(['repaired', 'legacy'].includes(values.expect), '--expect must be repaired or legacy.');
const outDir = path.resolve(values['out-dir']);
assert.equal(existsSync(outDir), false, 'The evidence directory must not already exist.');
const cliBin = realpathSync(values['cli-bin']);
const cliRoot = path.dirname(path.dirname(cliBin));
const packageJson = JSON.parse(readFileSync(path.join(cliRoot, 'package.json'), 'utf8'));
assert.equal(packageJson.name, '@tiangong-lca/cli');
const childEnv = { PATH: path.dirname(process.execPath), LANG: 'C', TZ: 'UTC' };
const sha256 = (bytes) => createHash('sha256').update(bytes).digest('hex');
const hashFile = (file) => (existsSync(file) ? sha256(readFileSync(file)) : null);
const writeJson = (file, value) => writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);
const readJson = (file) => JSON.parse(readFileSync(file, 'utf8'));
const readRows = (file) =>
  readFileSync(file, 'utf8')
    .trim()
    .split(/\r?\n/u)
    .map((line) => JSON.parse(line));
const fixtureFile = path.join(repoRoot, 'test/fixtures/source-classification.json');
const fixtureBytes = readFileSync(fixtureFile);
const fixture = JSON.parse(fixtureBytes);
const classificationOf = (payload) =>
  payload.sourceDataSet.sourceInformation.dataSetInformation.classificationInformation[
    'common:classification'
  ];
const setContainer = (payload, container) => {
  payload.sourceDataSet.sourceInformation.dataSetInformation.classificationInformation[
    'common:classification'
  ] = container;
};
const appliedClass = {
  '@level': '0',
  '@classId': '5',
  '#text': 'Publications and communications',
};
const git = (args) =>
  spawnSync('git', ['-C', cliRoot, ...args], {
    encoding: 'utf8',
    env: { ...childEnv, PATH: process.env.PATH ?? childEnv.PATH },
  });
const gitRoot = git(['rev-parse', '--show-toplevel']);
const hasCheckout = gitRoot.status === 0 && realpathSync(gitRoot.stdout.trim()) === cliRoot;
const sdkEntry = createRequire(cliBin).resolve('@tiangong-lca/tidas-sdk');
const sdkRoot = realpathSync(path.dirname(path.dirname(sdkEntry)));
const proof = {
  schema: 'tiangong-cli.source-classification-proof.v1',
  started_at: new Date().toISOString(),
  expectation: values.expect,
  status: 'running',
  runtime: {
    node: process.version,
    executable: realpathSync(process.execPath),
    executable_sha256: hashFile(realpathSync(process.execPath)),
    platform: `${process.platform}-${process.arch}`,
    package: packageJson.name,
    version: packageJson.version,
    cli_bin: cliBin,
    checkout_root: hasCheckout ? cliRoot : null,
    checkout_head: hasCheckout ? git(['rev-parse', 'HEAD']).stdout.trim() : null,
    checkout_branch: hasCheckout ? git(['branch', '--show-current']).stdout.trim() : null,
    checkout_status: hasCheckout ? git(['status', '--porcelain']).stdout.trim() : null,
    bin_sha256: hashFile(cliBin),
    built_main_sha256: hashFile(path.join(cliRoot, 'dist/src/main.js')),
    built_classification_sha256: hashFile(
      path.join(cliRoot, 'dist/src/lib/dataset-classification.js'),
    ),
    package_sha256: hashFile(path.join(cliRoot, 'package.json')),
    lockfile_sha256: hashFile(path.join(cliRoot, 'pnpm-lock.yaml')),
    sdk_version: readJson(path.join(sdkRoot, 'package.json')).version,
    sdk_package_sha256: hashFile(path.join(sdkRoot, 'package.json')),
    sdk_sources_schema_sha256: hashFile(path.join(sdkRoot, 'dist/schemas/tidas_sources.schema.js')),
  },
  proof_script_sha256: hashFile(fileURLToPath(import.meta.url)),
  fixture_sha256: sha256(fixtureBytes),
  child_environment_keys: Object.keys(childEnv),
  limits: {
    process_case:
      'Classification serialization only; minimal Process is not a schema-validity proof.',
    references:
      'Synthetic offline references; no remote existence or scientific acceptance claimed.',
  },
  commands: [],
  checks: [],
};
mkdirSync(outDir, { recursive: true });
writeFileSync(path.join(outDir, 'source-fixture.json'), fixtureBytes);

function check(name, action) {
  try {
    action();
    proof.checks.push({ name, status: 'passed' });
  } catch (error) {
    proof.checks.push({
      name,
      status: 'failed',
      message: error instanceof Error ? error.message : String(error),
    });
  }
}

function run(name, argv, input, artifact) {
  const startedAt = new Date().toISOString();
  const result = spawnSync(process.execPath, [cliBin, ...argv], {
    cwd: outDir,
    env: childEnv,
    encoding: 'utf8',
    shell: false,
    timeout: 30_000,
    maxBuffer: 4 * 1024 * 1024,
  });
  const caseDir = path.join(outDir, 'commands', name);
  mkdirSync(caseDir, { recursive: true });
  writeFileSync(path.join(caseDir, 'stdout.json'), result.stdout ?? '');
  writeFileSync(path.join(caseDir, 'stderr.txt'), result.stderr ?? '');
  const record = {
    name,
    actual_argv: [process.execPath, cliBin, ...argv],
    command: ['node', '<cli>/bin/tiangong-lca.js', ...argv],
    actual_cwd: outDir,
    started_at: startedAt,
    finished_at: new Date().toISOString(),
    exit_code: result.status,
    signal: result.signal,
    stdout: `commands/${name}/stdout.json`,
    stderr: `commands/${name}/stderr.txt`,
    input,
    input_sha256: hashFile(path.join(outDir, input)),
    report: artifact,
    report_sha256: hashFile(path.join(outDir, artifact)),
  };
  proof.commands.push(record);
  writeJson(path.join(caseDir, 'command.json'), record);
  assert.ifError(result.error);
  assert.equal(result.signal, null);
  assert.equal(result.stderr, '');
  const report = readJson(path.join(outDir, artifact));
  assert.deepEqual(JSON.parse(result.stdout), report, `${name}: stdout equals native report`);
  return { report, exitCode: result.status };
}

function validate(name, input, valid) {
  const output = `results/${name}`;
  const result = run(
    name,
    ['dataset', 'validate', '--input', input, '--type', 'source', '--out-dir', output, '--json'],
    input,
    `${output}/outputs/validation-report.json`,
  );
  check(`${name}: Source validation`, () => {
    assert.equal(result.exitCode, valid ? 0 : 1);
    assert.equal(result.report.counts.total, 1);
    assert.equal(result.report.counts.valid, valid ? 1 : 0);
    assert.equal(result.report.counts.invalid, valid ? 0 : 1);
    if (valid) {
      assert.equal(result.report.rows[0].validator, '@tiangong-lca/tidas-sdk/SourceSchema');
      for (const layer of Object.values(result.report.rows[0].validation_layers)) {
        assert.equal(layer.status, 'passed');
      }
    }
  });
}

function apply(name, payload, type = 'source', code = '5') {
  const caseDir = `cases/${name}`;
  mkdirSync(path.join(outDir, caseDir), { recursive: true });
  const input = `${caseDir}/input.json`;
  const decisions = `${caseDir}/decisions.json`;
  const output = `${caseDir}/classified.jsonl`;
  const resultDir = `results/${name}`;
  writeJson(path.join(outDir, input), payload);
  writeJson(path.join(outDir, decisions), [
    { row_index: 0, code, basis: 'Synthetic offline serialization proof.' },
  ]);
  const result = run(
    name,
    [
      'dataset',
      'classification',
      'apply',
      '--input',
      input,
      '--decisions',
      decisions,
      '--out',
      output,
      '--type',
      type,
      '--out-dir',
      resultDir,
      '--json',
    ],
    input,
    `${resultDir}/outputs/classification-apply-report.json`,
  );
  const evidenceFile = `${resultDir}/outputs/classification-apply-evidence.jsonl`;
  Object.assign(proof.commands.at(-1), {
    decisions_sha256: hashFile(path.join(outDir, decisions)),
    classified_rows: output,
    classified_rows_sha256: hashFile(path.join(outDir, output)),
    evidence: evidenceFile,
    evidence_sha256: hashFile(path.join(outDir, evidenceFile)),
  });
  writeJson(path.join(outDir, 'commands', name, 'command.json'), proof.commands.at(-1));
  const rows = readRows(path.join(outDir, output));
  assert.equal(rows.length, 1);
  return { ...result, payload: rows[0], output };
}

function assertApply(result, blocked) {
  assert.equal(result.exitCode, blocked ? 1 : 0);
  assert.equal(result.report.status, blocked ? 'blocked' : 'completed');
  assert.equal(result.report.counts.applied, blocked ? 0 : 1);
  assert.equal(result.report.counts.blockers, blocked ? 1 : 0);
}

function exerciseSource(name, payload, arrayContainer) {
  const input = `${name}-source.json`;
  writeJson(path.join(outDir, input), payload);
  validate(`${name}-input`, input, true);
  const result = apply(`${name}-apply`, payload);
  check(`${name}: applied without blockers`, () => assertApply(result, false));
  const expected = structuredClone(payload);
  if (values.expect === 'legacy') {
    setContainer(expected, {
      ...(arrayContainer ? {} : classificationOf(expected)),
      'common:class': [appliedClass],
    });
  } else if (arrayContainer) {
    classificationOf(expected)[0]['common:class'] = [appliedClass];
  } else {
    classificationOf(expected)['common:class'] = appliedClass;
  }
  check(`${name}: correct class shape and all other data preserved`, () =>
    assert.deepEqual(result.payload, expected),
  );
  validate(`${name}-output`, result.output, values.expect === 'repaired');
  const reapplied = apply(`${name}-reapply`, result.payload);
  check(`${name}: reapply succeeds and preserves payload bytes`, () => {
    assertApply(reapplied, false);
    assert.equal(
      hashFile(path.join(outDir, reapplied.output)),
      hashFile(path.join(outDir, result.output)),
    );
  });
  validate(`${name}-reapplied-output`, reapplied.output, values.expect === 'repaired');
}

try {
  exerciseSource('object-container', structuredClone(fixture), false);
  const singleArray = structuredClone(fixture);
  const objectContainer = classificationOf(singleArray);
  setContainer(singleArray, [
    { ...objectContainer, 'common:class': [objectContainer['common:class']] },
  ]);
  exerciseSource('single-array-container', singleArray, true);

  const multiple = structuredClone(singleArray);
  classificationOf(multiple).push({
    '@name': 'Independent synthetic categories',
    '@classes': 'https://example.invalid/categories/independent',
    'common:class': [{ '@level': '0', '@classId': 'custom', '#text': 'Independent category' }],
    'common:other': { 'proof:marker': 'Independent metadata must survive' },
  });
  writeJson(path.join(outDir, 'multiple-source.json'), multiple);
  validate('multiple-input', 'multiple-source.json', true);
  const ambiguous = apply('multiple-container-apply', multiple);
  check('multiple containers: blocked unchanged without a selector', () => {
    assertApply(ambiguous, values.expect === 'repaired');
    if (values.expect === 'repaired') {
      assert.deepEqual(ambiguous.payload, multiple);
      assert.equal(ambiguous.report.blockers[0].code, 'classification_container_missing');
    } else {
      const expected = structuredClone(multiple);
      setContainer(expected, { 'common:class': [appliedClass] });
      assert.deepEqual(ambiguous.payload, expected);
    }
  });
  validate('multiple-output', ambiguous.output, values.expect === 'repaired');

  const invalid = apply('invalid-decision', fixture, 'source', 'not-a-source-category');
  check('invalid decision: blocked and original Source unchanged', () => {
    assertApply(invalid, true);
    assert.equal(invalid.report.blockers[0].code, 'classification_decision_path_invalid');
    assert.deepEqual(invalid.payload, fixture);
  });
  validate('invalid-decision-output', invalid.output, true);

  const processPayload = {
    processDataSet: {
      processInformation: {
        dataSetInformation: {
          'common:UUID': '44444444-4444-4444-8444-444444444444',
          classificationInformation: {
            'common:classification': {
              '@name': 'Synthetic process categories',
              'common:class': [
                { '@level': '0', '@classId': 'S', '#text': 'Other service activities' },
              ],
            },
          },
        },
      },
    },
  };
  const processResult = apply('process-array-regression', processPayload, 'process', '1080');
  check('Process serialization: multi-level classification remains an array', () => {
    assertApply(processResult, false);
    const container =
      processResult.payload.processDataSet.processInformation.dataSetInformation
        .classificationInformation['common:classification'];
    assert.equal(container['@name'], 'Synthetic process categories');
    assert.equal(Array.isArray(container['common:class']), true);
    assert.equal(container['common:class'].length, 4);
    assert.equal(container['common:class'].at(-1)['@classId'], '1080');
    const expected = structuredClone(processPayload);
    expected.processDataSet.processInformation.dataSetInformation.classificationInformation[
      'common:classification'
    ]['common:class'] = container['common:class'];
    assert.deepEqual(processResult.payload, expected);
  });
  proof.status = proof.checks.every(({ status }) => status === 'passed') ? 'passed' : 'failed';
} catch (error) {
  proof.status = 'failed';
  proof.execution_error = error instanceof Error ? error.message : String(error);
} finally {
  proof.finished_at = new Date().toISOString();
  writeJson(path.join(outDir, 'proof.json'), proof);
  if (proof.status !== 'passed') process.exitCode = 1;
  console.log(JSON.stringify(proof, null, 2));
}
