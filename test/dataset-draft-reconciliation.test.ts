import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import {
  assertReconciliationFiles,
  observeDraftRows,
  reconciliationError,
  reconciliationFileFact,
  reconciliationOutputDirectory,
  writeDraftReconciliation,
} from '../src/lib/dataset-draft-reconciliation.js';
import { sha256Json } from '../src/lib/dataset-maintenance-contract.js';
import { CliError } from '../src/lib/errors.js';

const ID = '11111111-1111-4111-8111-111111111111';
const OWNER = '22222222-2222-4222-8222-222222222222';
const OTHER = '33333333-3333-4333-8333-333333333333';
const BODY = { processDataSet: { marker: 'original body', nested: { amount: 7 } } };
const EXPECTED = {
  id: ID,
  version: '01.00.001',
  user_id: OWNER,
  desired_sha256: sha256Json(BODY),
  process: true,
};

function row(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    id: ID,
    version: EXPECTED.version,
    user_id: OWNER,
    state_code: 0,
    json_ordered: structuredClone(BODY),
    model_id: null,
    model_version: null,
    team_id: null,
    review_id: null,
    ...overrides,
  };
}

function fixture() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'draft-reconciliation-'));
  return {
    root,
    file: path.join(root, 'original.json'),
    output: path.join(root, 'observation'),
    close: () => fs.rmSync(root, { recursive: true, force: true }),
  };
}

function invalid(error: unknown): boolean {
  assert.ok(error instanceof CliError);
  assert.equal(error.code, 'DATASET_DRAFT_RECONCILIATION_INVALID');
  assert.equal(error.exitCode, 2);
  assert.equal(error.details, undefined);
  return true;
}

test('equal owner Process observation retains full body and explicit null metadata with distinct hashes', () => {
  const original = row({ ignored_transport_header: 'PRIVATE_CANARY' });
  const observed = observeDraftRows([original], EXPECTED);
  assert.equal(observed.status, 'equal_eligible');
  assert.equal(observed.reason, 'equal_current_owner_draft');
  assert.deepEqual(observed.actual_row, row());
  assert.equal(observed.actual_payload_sha256, sha256Json(BODY));
  assert.equal(observed.actual_row_sha256, sha256Json(row()));
  assert.notEqual(observed.actual_row_sha256, observed.actual_payload_sha256);
  assert.equal(JSON.stringify(observed).includes('PRIVATE_CANARY'), false);
  assert.equal(original.ignored_transport_header, 'PRIVATE_CANARY');
});

test('equal Flow observation does not require Process metadata or manufacture it', () => {
  const flow = row();
  for (const key of ['model_id', 'model_version', 'team_id', 'review_id']) delete flow[key];
  const observed = observeDraftRows([flow], { ...EXPECTED, process: false });
  assert.equal(observed.status, 'equal_eligible');
  assert.deepEqual(observed.actual_row, flow);
});

test('body conflict, foreign owner and ineligible state remain separate observations', () => {
  const different = observeDraftRows([row({ json_ordered: { different: true } })], EXPECTED);
  assert.equal(different.status, 'different');
  assert.equal(different.reason, 'different_body');
  assert.equal(different.actual_payload_sha256, sha256Json({ different: true }));
  const foreign = observeDraftRows([row({ user_id: OTHER, state_code: 100 })], EXPECTED);
  assert.equal(foreign.status, 'foreign');
  assert.equal(foreign.reason, 'foreign_owner');
  assert.equal(foreign.actual_row?.user_id, OTHER);
  for (const state of [-1, 20, 100]) {
    const observed = observeDraftRows([row({ state_code: state })], EXPECTED);
    assert.equal(observed.status, 'ineligible');
    assert.equal(observed.reason, 'ineligible_state_or_metadata');
    assert.equal(observed.actual_row?.state_code, state);
  }
});

test('each nonnull Process relation independently makes an otherwise equal draft ineligible', () => {
  for (const key of ['model_id', 'model_version', 'team_id', 'review_id']) {
    const value = key === 'model_version' ? '01.00.002' : OTHER;
    const observed = observeDraftRows([row({ [key]: value })], EXPECTED);
    assert.equal(observed.status, 'ineligible', key);
    assert.equal(observed.reason, 'ineligible_state_or_metadata');
    assert.equal(
      observed.actual_row?.[key as keyof NonNullable<typeof observed.actual_row>],
      value,
    );
  }
});

test('absent and ambiguous exact reads disclose no manufactured row or hash', () => {
  for (const [rows, status, reason] of [
    [[], 'absent', 'not_visible'],
    [[row(), row()], 'indeterminate', 'ambiguous_rows'],
  ] as const) {
    assert.deepEqual(observeDraftRows([...rows], EXPECTED), {
      status,
      reason,
      actual_row: null,
      actual_row_sha256: null,
      actual_payload_sha256: null,
    });
  }
});

test('missing or malformed exact identity, ownership, state and body are indeterminate', () => {
  const incomplete: unknown[] = [null, [], 'PRIVATE_CANARY', false];
  for (const [key, values] of Object.entries({
    id: [undefined, OTHER],
    version: [undefined, '01.00.002'],
    user_id: [undefined, null, 7, '', 'PRIVATE_CANARY'],
    state_code: [undefined, null, '0', 0.5, Number.NaN, Infinity, Number.MAX_SAFE_INTEGER + 1],
    json_ordered: [undefined, null, [], 'PRIVATE_CANARY', 7],
  })) {
    for (const value of values) incomplete.push(row({ [key]: value }));
  }
  for (const malformed of incomplete) {
    const observed = observeDraftRows([malformed], EXPECTED);
    assert.equal(observed.status, 'indeterminate');
    assert.equal(observed.reason, 'invalid_or_incomplete_row');
    assert.equal(observed.actual_row, null);
    assert.equal(observed.actual_row_sha256, null);
    assert.equal(observed.actual_payload_sha256, null);
    assert.equal(JSON.stringify(observed).includes('PRIVATE_CANARY'), false);
  }
});

test('Process metadata must be observed explicitly: absent never means null', () => {
  for (const key of ['model_id', 'model_version', 'team_id', 'review_id']) {
    const missing = row();
    delete missing[key];
    for (const malformed of [
      missing,
      ...[undefined, 7, '', 'PRIVATE_CANARY'].map((value) => row({ [key]: value })),
    ]) {
      const observed = observeDraftRows([malformed], EXPECTED);
      assert.equal(observed.status, 'indeterminate', key);
      assert.equal(observed.reason, 'invalid_or_incomplete_row');
      assert.equal(observed.actual_row, null);
    }
  }
  assert.equal(observeDraftRows([row({ model_version: OTHER })], EXPECTED).status, 'indeterminate');
  assert.equal(
    observeDraftRows([row({ model_id: '01.00.001' })], EXPECTED).status,
    'indeterminate',
  );
});

test('file facts bind exact bytes, including empty files and optional absence', () => {
  const f = fixture();
  try {
    assert.deepEqual(reconciliationFileFact(f.file, true), {
      path: f.file,
      sha256: null,
      bytes: null,
    });
    assert.throws(() => reconciliationFileFact(f.file), { code: 'ENOENT' });
    const bytes = Buffer.from('original\n');
    fs.writeFileSync(f.file, bytes);
    assert.deepEqual(reconciliationFileFact(f.file), {
      path: f.file,
      sha256: createHash('sha256').update(bytes).digest('hex'),
      bytes: bytes.length,
    });
    fs.writeFileSync(f.file, '');
    assert.equal(reconciliationFileFact(f.file).bytes, 0);
  } finally {
    f.close();
  }
});

test('fingerprint rechecks reject changed, disappeared or newly created original evidence', () => {
  const f = fixture();
  try {
    const absent = reconciliationFileFact(f.file, true);
    assert.doesNotThrow(() => assertReconciliationFiles([absent]));
    fs.writeFileSync(f.file, 'original');
    assert.throws(() => assertReconciliationFiles([absent]), invalid);
    const original = reconciliationFileFact(f.file);
    assert.doesNotThrow(() => assertReconciliationFiles([original]));
    fs.writeFileSync(f.file, 'changed!');
    assert.throws(() => assertReconciliationFiles([original]), invalid);
    fs.unlinkSync(f.file);
    assert.throws(() => assertReconciliationFiles([original]), { code: 'ENOENT' });
  } finally {
    f.close();
  }
});

test('symlinks, directories and inputs exceeding 32 MiB cannot supply evidence', () => {
  const f = fixture();
  try {
    fs.writeFileSync(f.file, 'original');
    const link = path.join(f.root, 'link');
    fs.symlinkSync(f.file, link, 'file');
    assert.throws(() => reconciliationFileFact(link));
    assert.throws(() => reconciliationFileFact(link, true));
    assert.throws(() => reconciliationFileFact(f.root), invalid);
    const descriptor = fs.openSync(f.file, 'r+');
    fs.ftruncateSync(descriptor, 32 * 1024 * 1024 + 1);
    fs.closeSync(descriptor);
    assert.throws(() => reconciliationFileFact(f.file), invalid);
  } finally {
    f.close();
  }
});

test('changes during byte reads or metadata observation fail closed and release the file descriptor', (t) => {
  const f = fixture();
  const read = fs.readFileSync;
  const stat = fs.fstatSync;
  try {
    fs.writeFileSync(f.file, 'original');
    for (const mode of ['short-read', 'changed-time', 'read-error']) {
      let descriptor = -1;
      let stats = 0;
      t.mock.method(fs, 'fstatSync', (fd: number, options?: { bigint?: boolean }) => {
        assert.equal(options?.bigint, true);
        descriptor = fd;
        const value = stat(fd, { bigint: true });
        if (mode === 'changed-time' && ++stats === 2) value.mtimeNs += 1n;
        return value;
      });
      if (mode !== 'changed-time') {
        t.mock.method(fs, 'readFileSync', (file: number) => {
          if (mode === 'read-error') throw new Error('synthetic local I/O failure');
          return (read(file) as Buffer).subarray(1);
        });
      }
      syncBuiltinESMExports();
      if (mode === 'read-error')
        assert.throws(() => reconciliationFileFact(f.file), /synthetic local I\/O failure/u);
      else assert.throws(() => reconciliationFileFact(f.file), invalid);
      assert.throws(() => stat(descriptor), { code: 'EBADF' });
      t.mock.restoreAll();
      syncBuiltinESMExports();
    }
  } finally {
    t.mock.restoreAll();
    syncBuiltinESMExports();
    f.close();
  }
});

test('replacement between path precheck and open fails before reading and closes the opened descriptor', (t) => {
  const f = fixture();
  const stat = fs.fstatSync;
  try {
    fs.writeFileSync(f.file, 'original evidence');
    for (const replacement of ['nonregular', 'different-device', 'different-inode']) {
      let descriptor = -1;
      let reads = 0;
      t.mock.method(fs, 'fstatSync', (fd: number, options?: { bigint?: boolean }) => {
        assert.equal(options?.bigint, true);
        descriptor = fd;
        const value = stat(fd, { bigint: true });
        if (replacement === 'nonregular') value.isFile = () => false;
        if (replacement === 'different-device') value.dev += 1n;
        if (replacement === 'different-inode') value.ino += 1n;
        return value;
      });
      t.mock.method(fs, 'readFileSync', () => {
        reads += 1;
        throw new Error('A substituted file must not be read.');
      });
      syncBuiltinESMExports();
      assert.throws(() => reconciliationFileFact(f.file), invalid);
      assert.equal(reads, 0, replacement);
      assert.throws(() => stat(descriptor), { code: 'EBADF' });
      t.mock.restoreAll();
      syncBuiltinESMExports();
    }
  } finally {
    t.mock.restoreAll();
    syncBuiltinESMExports();
    f.close();
  }
});

test('distinct 64-bit file IDs that collapse to the same number still reject replacement before reading', (t) => {
  const f = fixture();
  const lstat = fs.lstatSync;
  const stat = fs.fstatSync;
  const first = 2n ** 60n;
  const second = first + 1n;
  let descriptor = -1;
  let reads = 0;
  try {
    fs.writeFileSync(f.file, 'synthetic');
    assert.notEqual(first, second);
    assert.equal(Number(first), Number(second), 'this is the numeric-ID RED condition');
    t.mock.method(fs, 'lstatSync', (file: fs.PathLike, options?: { bigint?: boolean }) => {
      assert.equal(options?.bigint, true);
      return Object.assign(lstat(file, { bigint: true }), { ino: first });
    });
    t.mock.method(fs, 'fstatSync', (fd: number, options?: { bigint?: boolean }) => {
      assert.equal(options?.bigint, true);
      descriptor = fd;
      return Object.assign(stat(fd, { bigint: true }), { ino: second });
    });
    t.mock.method(fs, 'readFileSync', () => {
      reads += 1;
      throw new Error('A replaced large-ID file must not be read.');
    });
    syncBuiltinESMExports();
    assert.throws(() => reconciliationFileFact(f.file), invalid);
    assert.equal(reads, 0);
    assert.throws(() => stat(descriptor), { code: 'EBADF' });
  } finally {
    t.mock.restoreAll();
    syncBuiltinESMExports();
    f.close();
  }
});

test('output containment follows physical parent paths and rejects the ledger itself and descendants', () => {
  const f = fixture();
  try {
    const ledger = path.join(f.root, 'ledger');
    fs.mkdirSync(ledger);
    const physicalLedger = fs.realpathSync(ledger);
    const link = path.join(f.root, 'ledger-link');
    fs.symlinkSync(ledger, link, 'junction');
    for (const output of [
      ledger,
      path.join(ledger, 'observation'),
      path.join(link, 'observation'),
    ]) {
      assert.throws(() => reconciliationOutputDirectory(output, physicalLedger), invalid);
    }
    assert.equal(
      reconciliationOutputDirectory(f.output, physicalLedger),
      path.join(fs.realpathSync(f.root), 'observation'),
    );
    assert.equal(
      reconciliationOutputDirectory(path.join(f.root, 'ledger-sibling'), physicalLedger),
      path.join(fs.realpathSync(f.root), 'ledger-sibling'),
    );
  } finally {
    f.close();
  }
});

test('a lexical ledger below a symlinked state ancestor cannot admit physical or alias output paths', () => {
  const f = fixture();
  try {
    const state = path.join(f.root, 'physical-state');
    fs.mkdirSync(state);
    const alias = path.join(f.root, 'state-alias');
    fs.symlinkSync(state, alias, 'junction');
    const ledger = path.join(state, 'execution-registry');
    fs.mkdirSync(ledger);
    const lexicalLedger = path.join(alias, 'execution-registry');
    for (const output of [
      ledger,
      lexicalLedger,
      path.join(ledger, 'observation'),
      path.join(lexicalLedger, 'observation'),
    ]) {
      assert.throws(
        () => reconciliationOutputDirectory(output, lexicalLedger),
        (error) => {
          invalid(error);
          assert.match((error as Error).message, /outside the execution ledger/u);
          return true;
        },
      );
    }
    assert.equal(
      reconciliationOutputDirectory(f.output, lexicalLedger),
      path.join(fs.realpathSync(f.root), 'observation'),
    );
    assert.equal(fs.readdirSync(ledger).length, 0);
  } finally {
    f.close();
  }
});

test('missing ledger suffixes resolve through the existing physical ancestor without creating them', () => {
  const f = fixture();
  try {
    const state = path.join(f.root, 'physical-state');
    fs.mkdirSync(state);
    const alias = path.join(f.root, 'state-alias');
    fs.symlinkSync(state, alias, 'junction');
    const missingLedger = path.join(alias, 'not-created', 'execution-registry');
    assert.equal(
      reconciliationOutputDirectory(f.output, missingLedger),
      path.join(fs.realpathSync(f.root), 'observation'),
    );
    assert.equal(fs.existsSync(path.join(state, 'not-created')), false);

    fs.mkdirSync(path.join(state, 'not-created'));
    const physicalLedger = path.join(state, 'not-created', 'execution-registry');
    for (const output of [physicalLedger, missingLedger]) {
      assert.throws(
        () => reconciliationOutputDirectory(output, missingLedger),
        (error) => {
          invalid(error);
          assert.match((error as Error).message, /outside the execution ledger/u);
          return true;
        },
      );
    }
    assert.equal(fs.existsSync(physicalLedger), false);
  } finally {
    f.close();
  }
});

test('case aliases of present or missing ledger roots stay reserved without changing output spelling', () => {
  for (const [ledgerName, aliasName] of [
    ['registry', 'REGISTRY'],
    ['registré', 'REGISTRE\u0301'],
  ] as const) {
    const f = fixture();
    try {
      const ledger = path.join(f.root, ledgerName);
      const alias = path.join(f.root, aliasName);
      assert.throws(() => reconciliationOutputDirectory(alias, ledger), invalid);
      assert.equal(fs.existsSync(alias), false);
      assert.equal(fs.existsSync(ledger), false);
      fs.mkdirSync(ledger);
      if (!fs.existsSync(alias)) fs.mkdirSync(alias);
      for (const output of [alias, path.join(alias, 'observation')]) {
        assert.throws(() => reconciliationOutputDirectory(output, ledger), invalid);
      }
      const sibling = path.join(f.root, 'REGISTRY-SIBLING');
      assert.equal(
        reconciliationOutputDirectory(sibling, ledger),
        path.join(fs.realpathSync(f.root), 'REGISTRY-SIBLING'),
      );
      assert.equal(fs.existsSync(sibling), false);
    } finally {
      f.close();
    }
  }
});

test('non-directory ledger ancestors fail rather than being treated as an absent registry', () => {
  const f = fixture();
  try {
    fs.writeFileSync(f.file, 'original evidence');
    assert.throws(
      () => reconciliationOutputDirectory(f.output, path.join(f.file, 'execution-registry')),
      (error: unknown) =>
        error instanceof CliError
          ? invalid(error)
          : (error as NodeJS.ErrnoException).code === 'ENOTDIR',
    );
    assert.equal(fs.existsSync(f.output), false);
  } finally {
    f.close();
  }
});

test('a Windows ENOENT child beneath an existing regular file cannot be reconstructed as a missing ledger directory', (t) => {
  const f = fixture();
  const realpath = fs.realpathSync;
  try {
    fs.writeFileSync(f.file, 'synthetic');
    const ledger = path.join(f.file, 'execution-registry');
    t.mock.method(fs, 'realpathSync', (file: fs.PathLike) => {
      if (String(file) === ledger)
        throw Object.assign(new Error('synthetic Windows child-under-file ENOENT'), {
          code: 'ENOENT',
        });
      return realpath(file);
    });
    syncBuiltinESMExports();
    assert.throws(() => reconciliationOutputDirectory(f.output, ledger), invalid);
    assert.throws(() => reconciliationOutputDirectory(f.output, f.file), invalid);
    assert.equal(fs.existsSync(f.output), false);
    t.mock.restoreAll();
    syncBuiltinESMExports();
    const actualLedger = path.join(f.root, 'actual-registry');
    fs.mkdirSync(actualLedger);
    assert.equal(
      reconciliationOutputDirectory(f.output, actualLedger),
      path.join(realpath(f.root), 'observation'),
    );
    assert.equal(
      reconciliationOutputDirectory(f.output, path.join(f.root, 'missing', 'registry')),
      path.join(realpath(f.root), 'observation'),
    );
    assert.equal(fs.existsSync(path.join(f.root, 'missing')), false);
  } finally {
    t.mock.restoreAll();
    syncBuiltinESMExports();
    f.close();
  }
});

test('existing output directories, files and symlink aliases fail during preflight with original bytes intact', () => {
  const f = fixture();
  try {
    fs.mkdirSync(f.output);
    fs.writeFileSync(f.file, 'original evidence');
    const link = path.join(f.root, 'output-alias');
    fs.symlinkSync(f.output, link, 'junction');
    for (const output of [f.output, f.file, link]) {
      assert.throws(
        () => reconciliationOutputDirectory(output, path.join(f.root, 'not-created-ledger')),
        (error) => {
          invalid(error);
          assert.match((error as Error).message, /output already exists/u);
          return true;
        },
      );
    }
    assert.equal(fs.readFileSync(f.file, 'utf8'), 'original evidence');
    assert.equal(fs.readdirSync(f.output).length, 0);
  } finally {
    f.close();
  }
});

test('published observations are private immutable artifacts and rejected reruns preserve original bytes', () => {
  const f = fixture();
  try {
    const observed = observeDraftRows([row()], EXPECTED);
    writeDraftReconciliation(f.output, observed);
    const file = path.join(f.output, 'reconciliation.json');
    const before = fs.readFileSync(file);
    assert.deepEqual(JSON.parse(before.toString()), observed);
    if (process.platform !== 'win32') {
      assert.equal(fs.statSync(f.output).mode & 0o777, 0o700);
      assert.equal(fs.statSync(file).mode & 0o777, 0o600);
    }
    assert.throws(() => writeDraftReconciliation(f.output, { replacement: true }), {
      code: 'EEXIST',
    });
    assert.deepEqual(fs.readFileSync(file), before);
    const link = path.join(f.root, 'existing-link');
    fs.symlinkSync(f.output, link, 'junction');
    assert.throws(() => writeDraftReconciliation(link, { replacement: true }), { code: 'EEXIST' });
    assert.deepEqual(fs.readFileSync(file), before);
  } finally {
    f.close();
  }
});

test('output file reservation races preserve the other writer', (t) => {
  const f = fixture();
  const mkdir = fs.mkdirSync;
  try {
    t.mock.method(fs, 'mkdirSync', (directory: fs.PathLike, options?: fs.MakeDirectoryOptions) => {
      const result = mkdir(directory, options);
      fs.writeFileSync(path.join(String(directory), 'reconciliation.json'), 'winning writer');
      return result;
    });
    syncBuiltinESMExports();
    assert.throws(() => writeDraftReconciliation(f.output, { replacement: true }), {
      code: 'EEXIST',
    });
    assert.equal(
      fs.readFileSync(path.join(f.output, 'reconciliation.json'), 'utf8'),
      'winning writer',
    );
  } finally {
    t.mock.restoreAll();
    syncBuiltinESMExports();
    f.close();
  }
});

test('publication failure closes the owned descriptor', (t) => {
  const f = fixture();
  const stat = fs.fstatSync;
  let descriptor = -1;
  try {
    t.mock.method(fs, 'writeFileSync', (fd: number) => {
      descriptor = fd;
      throw new Error('synthetic local write failure');
    });
    syncBuiltinESMExports();
    assert.throws(
      () => writeDraftReconciliation(f.output, { status: 'indeterminate' }),
      /synthetic local write failure/u,
    );
    assert.throws(() => stat(descriptor), { code: 'EBADF' });
  } finally {
    t.mock.restoreAll();
    syncBuiltinESMExports();
    f.close();
  }
});

test('oversized evidence is rejected before creating output and native validation errors retain no details', () => {
  const f = fixture();
  try {
    assert.throws(
      () => writeDraftReconciliation(f.output, { large: 'x'.repeat(32 * 1024 * 1024) }),
      invalid,
    );
    assert.equal(fs.existsSync(f.output), false);
    assert.throws(() => reconciliationError('bounded validation failure'), invalid);
  } finally {
    f.close();
  }
});
