import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  appendFileSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  readdirSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { isAbsolute, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const HELPER = fileURLToPath(import.meta.url);
const ACTOR = '11111111-1111-4111-8111-111111111111';
const EMAIL = 'user@example.com';
const STUB_ENV = 'TIANGONG_DATASET_READ_PACKED_SMOKE';

function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === 'object')
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, stable(value[key])]),
    );
  return value;
}
function bytesHash(value) {
  return createHash('sha256').update(value).digest('hex');
}
function bodyHash(value) {
  return bytesHash(JSON.stringify(stable(value)));
}

// This preload never falls through to the native network implementation.
if (process.env[STUB_ENV]) {
  const config = JSON.parse(readFileSync(process.env[STUB_ENV], 'utf8'));
  globalThis.fetch = async (input, init) => {
    const url = new URL(
      typeof input === 'string' || input instanceof URL ? String(input) : input.url,
    );
    const method = (
      init?.method ?? (input instanceof Request ? input.method : 'GET')
    ).toUpperCase();
    appendFileSync(config.requests_file, JSON.stringify({ method, url: url.href }) + '\n');
    assert.equal(method, 'GET', 'packed read commands must never send mutations');
    assert.equal(
      url.origin,
      'https://example.supabase.co',
      'synthetic fetch blocks foreign destinations',
    );
    let body;
    if (url.pathname === '/auth/v1/user') body = { id: ACTOR, email: EMAIL, role: 'authenticated' };
    else {
      assert.equal(
        url.pathname,
        '/rest/v1/processes',
        'synthetic fetch blocks unselected relations, RPCs and Edge routes',
      );
      assert.equal(url.searchParams.get('id'), `eq.${config.row.id}`);
      assert.equal(url.searchParams.get('version'), `eq.${config.row.version}`);
      assert.equal(url.searchParams.get('limit'), '2');
      assert.equal(url.searchParams.has('order'), false, 'an exact read cannot request latest');
      if (config.command === 'reconcile') {
        assert.equal(url.searchParams.get('user_id'), 'eq.' + ACTOR);
        assert.equal(url.searchParams.get('state_code'), 'eq.0');
      } else {
        assert.equal(
          url.searchParams.get('or'),
          '(and(state_code.gte.100,state_code.lt.200),and(state_code.eq.0,user_id.eq.' +
            ACTOR +
            '))',
        );
      }
      body = [config.row];
    }
    return new Response(JSON.stringify(body), { headers: { 'content-type': 'application/json' } });
  };
}

function cleanEnvironment() {
  const env = {};
  for (const name of [
    'COMSPEC',
    'ComSpec',
    'PATH',
    'Path',
    'PATHEXT',
    'SYSTEMROOT',
    'SystemRoot',
    'TEMP',
    'TMP',
    'TMPDIR',
  ])
    if (process.env[name] !== undefined) env[name] = process.env[name];
  return env;
}

function runCommand(nodePath, cliLauncher, cwd, env, argv, requestsFile) {
  const result = spawnSync(
    nodePath,
    ['--import', pathToFileURL(HELPER).href, cliLauncher, ...argv],
    {
      cwd,
      env,
      encoding: 'utf8',
      timeout: 30_000,
      maxBuffer: 4 * 1024 * 1024,
    },
  );
  let report = null;
  try {
    report = JSON.parse(result.stdout);
  } catch {
    /* A failing packed command may return only its error envelope. */
  }
  return {
    status: result.status,
    signal: result.signal,
    error: result.error ? { name: result.error.name, message: result.error.message } : null,
    stdout: result.stdout,
    stderr: result.stderr,
    report,
    requests: readFileSync(requestsFile, 'utf8')
      .split('\n')
      .filter(Boolean)
      .map((line) => JSON.parse(line)),
  };
}

function assertCommand(result, expectedVersion, expectedRow, credential) {
  assert.equal(result.error, null, JSON.stringify(result));
  assert.equal(
    result.status,
    0,
    `Packed public command failed:\n${result.stderr || result.stdout}`,
  );
  assert.equal(result.stderr, '');
  const report = result.report;
  assert.equal(report.mode, 'read_only');
  assert.equal(report.status, 'observed');
  assert.equal(report.authorization_granted, false);
  assert.equal(
    report.identity.cli.package_version,
    expectedVersion,
    'identity must resolve the installed package version',
  );
  assert.equal(report.identity.assertions.mode, 'intent-bound');
  assert.equal(report.identity.identity.user_id, ACTOR);
  assert.deepEqual(report.rows[0].actual_row.json_ordered, expectedRow.json_ordered);
  for (const key of [
    'id',
    'version',
    'user_id',
    'state_code',
    'model_id',
    'model_version',
    'team_id',
    'review_id',
  ])
    assert.equal(
      report.rows[0].actual_row[key],
      expectedRow[key],
      `complete observed metadata: ${key}`,
    );
  assert.equal(report.rows[0].actual_payload_sha256, bodyHash(expectedRow.json_ordered));
  assert.equal(
    result.stdout.includes(credential),
    false,
    'credentials cannot appear in public CLI output',
  );
  assert.ok(result.requests.some((request) => new URL(request.url).pathname === '/auth/v1/user'));
  const reads = result.requests.filter(
    (request) => new URL(request.url).pathname === '/rest/v1/processes',
  );
  assert.equal(reads.length, 1, 'each read command performs exactly one selected Process lookup');
  assert.ok(result.requests.every((request) => request.method === 'GET'));
}

/** Exercise the actual installed launcher without source imports, builds or live requests. */
export function runDatasetReadPackedSmoke({
  nodePath = process.execPath,
  cliLauncher,
  expectedVersion = '0.1.28',
  assertSuccess = true,
  keepFixture = false,
  workRoot = tmpdir(),
}) {
  assert.ok(isAbsolute(nodePath));
  assert.ok(isAbsolute(cliLauncher));
  const root = mkdtempSync(join(workRoot, 'dataset-read-packed-smoke-'));
  try {
    const caller = join(root, 'empty-caller');
    const evidence = join(root, 'synthetic-evidence');
    mkdirSync(caller);
    mkdirSync(evidence);
    const payload = JSON.parse(
      readFileSync(
        new URL('../fixtures/tidas-sdk/test-data/process-annual-volume.json', import.meta.url),
        'utf8',
      ),
    );
    const dataset = payload.processDataSet;
    const id = dataset.processInformation.dataSetInformation['common:UUID'];
    const version =
      dataset.administrativeInformation.publicationAndOwnership['common:dataSetVersion'];
    const row = {
      id,
      version,
      user_id: ACTOR,
      state_code: 0,
      json_ordered: payload,
      model_id: null,
      model_version: null,
      team_id: null,
      review_id: null,
      modified_at: null,
    };
    const contract = {
      schema_version: 'dataset-save-draft-execution-contract.v1',
      execution_id: 'synthetic-packed-original-request',
      project_ref: 'example',
      target_mode: 'owner_draft',
      owner: { user_id: ACTOR, email: EMAIL, state_code: 0 },
      actions: [
        {
          action_id: 'original-process',
          table: 'processes',
          id,
          version,
          desired_sha256: bodyHash(payload),
          expected_operation: 'insert',
          before_sha256: null,
          dependency_action_ids: [],
        },
      ],
    };
    const inputFile = join(evidence, 'original-process.json');
    const contractFile = join(evidence, 'original-contract.json');
    const scopeFile = join(evidence, 'scope.json');
    writeFileSync(inputFile, JSON.stringify(payload));
    writeFileSync(contractFile, JSON.stringify(contract));
    const originals = [inputFile, contractFile].map((file) => ({
      file,
      bytes: readFileSync(file),
    }));
    const scope = {
      schema_version: 'dataset-exact-read-scope.v1',
      project_ref: 'example',
      actor: { user_id: ACTOR },
      sources: originals.map(({ file, bytes }) => ({ path: file, sha256: bytesHash(bytes) })),
      rows: [{ table: 'processes', id, version, expected_model: null }],
    };
    writeFileSync(scopeFile, JSON.stringify(scope));
    originals.push({ file: scopeFile, bytes: readFileSync(scopeFile) });
    const credential =
      'e30.' +
      Buffer.from(JSON.stringify({ sub: ACTOR, email: EMAIL })).toString('base64url') +
      '.synthetic-signature';
    const baseEnv = {
      ...cleanEnvironment(),
      TIANGONG_LCA_API_BASE_URL: 'https://example.supabase.co',
      TIANGONG_LCA_AUTH_MODE: 'access-token',
      TIANGONG_LCA_SUPABASE_PUBLISHABLE_KEY: 'synthetic-public-key',
      TIANGONG_LCA_ACCESS_TOKEN: credential,
      TIANGONG_LCA_DISABLE_SESSION_CACHE: 'true',
      XDG_STATE_HOME: join(root, 'state'),
    };
    const invoke = (name, argv) => {
      const requestsFile = join(root, `${name}-requests.jsonl`);
      const configFile = join(root, `${name}-fetch.json`);
      writeFileSync(requestsFile, '');
      writeFileSync(
        configFile,
        JSON.stringify({ row, requests_file: requestsFile, command: name }),
      );
      return runCommand(
        nodePath,
        cliLauncher,
        caller,
        { ...baseEnv, [STUB_ENV]: configFile },
        argv,
        requestsFile,
      );
    };
    const reconcileOutput = join(root, 'reconciliation');
    const readOutput = join(root, 'exact-read');
    const reconcile = invoke('reconcile', [
      'dataset',
      'reconcile-draft',
      '--input',
      inputFile,
      '--execution-contract',
      contractFile,
      '--out-dir',
      reconcileOutput,
      '--type',
      'process',
      '--json',
    ]);
    const readExact = invoke('read-exact', [
      'dataset',
      'read-exact',
      '--scope-file',
      scopeFile,
      '--scope-sha256',
      bytesHash(readFileSync(scopeFile)),
      '--out-dir',
      readOutput,
      '--json',
    ]);
    if (assertSuccess) {
      assertCommand(reconcile, expectedVersion, row, credential);
      assertCommand(readExact, expectedVersion, row, credential);
      assert.equal(reconcile.report.schema_version, 'dataset-draft-reconciliation.v1');
      assert.equal(reconcile.report.rows[0].status, 'equal_eligible');
      assert.equal(reconcile.report.historical_execution_changed, false);
      assert.equal(reconcile.report.rows[0].history.attempt_consumed, false);
      assert.equal(reconcile.report.rows[0].history.outcome, null);
      assert.equal(reconcile.report.rows[0].history.event_count, 0);
      assert.equal(readExact.report.schema_version, 'dataset-exact-read-report.v1');
      assert.equal(readExact.report.adoption_performed, false);
      assert.equal(readExact.report.rows[0].status, 'found');
      assert.equal(readExact.report.rows[0].model_association, null);
      assert.deepEqual(
        JSON.parse(readFileSync(join(reconcileOutput, 'reconciliation.json'), 'utf8')),
        reconcile.report,
      );
      assert.deepEqual(
        JSON.parse(readFileSync(join(readOutput, 'read-exact.json'), 'utf8')),
        readExact.report,
      );
    }
    for (const original of originals) assert.deepEqual(readFileSync(original.file), original.bytes);
    assert.deepEqual(
      readdirSync(caller),
      [],
      'public reads must not depend on or write into the empty caller workspace',
    );
    assert.equal(
      existsSync(baseEnv.XDG_STATE_HOME),
      false,
      'read commands must not create session or execution state',
    );
    return { fixture_root: root, reconcile, read_exact: readExact };
  } finally {
    if (!keepFixture) rmSync(root, { recursive: true, force: true });
  }
}

if (
  !process.env[STUB_ENV] &&
  process.argv.includes('--cli') &&
  process.argv[1] &&
  resolve(process.argv[1]) === HELPER
) {
  const argv = process.argv.slice(2);
  const value = (flag) => argv[argv.indexOf(flag) + 1];
  const result = runDatasetReadPackedSmoke({
    nodePath: argv.includes('--node') ? value('--node') : process.execPath,
    cliLauncher: value('--cli'),
    expectedVersion: argv.includes('--version') ? value('--version') : '0.1.28',
    assertSuccess: !argv.includes('--allow-failure'),
    keepFixture: argv.includes('--keep-fixture'),
    workRoot: argv.includes('--work-root') ? value('--work-root') : tmpdir(),
  });
  process.stdout.write(JSON.stringify(result, null, 2) + '\n');
}
